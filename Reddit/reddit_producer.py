import time
import threading
import praw
import json
import os
from dotenv import load_dotenv
from confluent_kafka import Producer

# Cargar las variables de entorno desde el archivo .env
load_dotenv('reddit.env')

# Configuración del productor de Kafka usando las variables de entorno
conf = {'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS')}
producer = Producer(conf)

# Set para almacenar los IDs de los posts y comentarios capturados (no permite duplicados)
captured_posts_ids = set()
processed_comments = {}  # Diccionario para almacenar comentarios por ID

# Función para enviar los comentarios a Kafka
def send_to_kafka(topic, data):
    producer.produce(topic, value=json.dumps(data))
    producer.poll(0)

# Función para capturar comentarios de todos los posts en la lista
def listen_to_comments_for_all_posts():
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )

    while True:
        for post_id in list(captured_posts_ids):  # Convertir a lista para iterar de forma segura
            submission = reddit.submission(id=post_id)
            submission.comments.replace_more(limit=None)  # Cargar todos los comentarios
            for comment in submission.comments.list():
                # Revisar si el comentario ya existe y si ha cambiado
                if (comment.id not in processed_comments) or (processed_comments[comment.id] != comment.body):
                    comment_data = {
                        'type': 'comment',
                        'post_id': submission.id,
                        'comment_id': comment.id,
                        'comment_body': comment.body.strip() if comment.body else 'sin texto',
                        'comment_author': str(comment.author),
                        'created': comment.created_utc,
                        'edited': comment.edited
                    }
                    send_to_kafka('reddit_data', comment_data)
                    print(f"Comentario enviado: {comment.body}")

                    # Actualizar el diccionario de comentarios procesados
                    processed_comments[comment.id] = comment.body
        time.sleep(5)  # Pausa para revisar los comentarios de los posts de forma periódica

# Función para capturar nuevas publicaciones y agregar sus IDs al set
def get_reddit_data(subreddit_name):
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )
    subreddit = reddit.subreddit(subreddit_name)

    print(f"Escuchando publicaciones de: {subreddit_name}")

    for submission in subreddit.stream.submissions(skip_existing=True):
        if submission.id not in captured_posts_ids:  # Evitar procesar posts duplicados
            # Procesar el post
            post_data = {
                'type': 'post',
                'post_id': submission.id,
                'title': submission.title.strip(),
                'author': str(submission.author),
                'url': submission.url,
                'created': submission.created_utc,
                'subreddit': str(submission.subreddit)
            }
            send_to_kafka('reddit_data', post_data)
            print(f"Post enviado: {submission.title}")

            # Agregar el post ID al set para capturar comentarios más adelante
            captured_posts_ids.add(submission.id)

        time.sleep(2)  # Pausa para no saturar la API de Reddit

# Función para pedir al usuario los subreddits
def obtener_subreddits():
    subreddits = input("Introduce los subreddits separados por comas: ").split(',')
    subreddits = [subreddit.strip() for subreddit in subreddits]  # Eliminar espacios
    print(f"Has ingresado los siguientes subreddits: {', '.join(subreddits)}")
    confirmar = input("¿Deseas continuar? (S/N): ").strip().lower()
    if confirmar == 's':
        return subreddits
    else:
        print("Cancelado por el usuario.")
        exit()

# Función principal
if __name__ == "__main__":

    # Crear hilo para capturar los comentarios de todos los posts almacenados
    comment_thread = threading.Thread(target=listen_to_comments_for_all_posts)
    comment_thread.start()

    # Pedir al usuario los subreddits y crear un hilo por cada uno
    subreddits = obtener_subreddits()
    threads = []

    for subreddit in subreddits:
        thread = threading.Thread(target=get_reddit_data, args=(subreddit,))
        thread.start()
        threads.append(thread)

    # Esperar a que todos los hilos terminen
    for thread in threads:
        thread.join()

    # Asegurarse de que todos los mensajes se han enviado
    producer.flush()
