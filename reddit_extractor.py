import time
import threading
import praw
import json
from confluent_kafka import Producer

# Configuración del productor de Kafka
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# Función para enviar los datos a Kafka
def send_to_kafka(topic, data):
    producer.produce(topic, value=json.dumps(data))
    producer.poll(0)

# Función para escuchar los comentarios de un post
def listen_to_comments(submission):
    submission.comments.replace_more(limit=None)  # Cargar todos los comentarios
    for comment in submission.comments.list():
        comment_data = {
            'type': 'comment',
            'post_id': submission.id,
            'comment_id': comment.id,
            'comment_body': comment.body.strip() if comment.body else 'sin texto',
            'comment_author': str(comment.author),
            'created': comment.created_utc
        }
        print(f"Capturando comentario de post {submission.id}: {comment.body}")  # Log para depuración
        send_to_kafka('reddit_data', comment_data)
    # Pausa para evitar saturar la API
    time.sleep(1)

# Función para extraer publicaciones de un subreddit y lanzar hilos para comentarios
def get_reddit_data(subreddit_name):
    reddit = praw.Reddit(
        client_id='Tbn0oC6kfayGMfgKjUvXbw',
        client_secret='pOrII9VcFP6G4ZvEm5SjF46EGFmH2Q',
        user_agent='INDA/v0.1',
        redirect_uri='http://localhost:8000',  # La URI de redirección de tu app
        scopes=['read', 'identity']  # Asegúrate de incluir el permiso "read"
    )
    subreddit = reddit.subreddit(subreddit_name)
    
    print(f"Escuchando publicaciones de: {subreddit_name}")

    for submission in subreddit.stream.submissions(skip_existing=True):
        # Procesar el post
        post_data = {
            'type': 'post',
            'title': submission.title.strip(),
            'author': str(submission.author),
            'url': submission.url,
            'created': submission.created_utc,
            'subreddit': str(submission.subreddit)
        }
        send_to_kafka('reddit_data', post_data)

        # Iniciar hilo para escuchar comentarios del post
        thread = threading.Thread(target=listen_to_comments, args=(submission,))
        thread.start()

        # Pausa para evitar saturar la API de Reddit
        time.sleep(2)  # Ajusta el tiempo si es necesario

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
    subreddits = obtener_subreddits()  # Pedir al usuario los subreddits
    threads = []

    # Crear un hilo por cada subreddit
    for subreddit in subreddits:
        thread = threading.Thread(target=get_reddit_data, args=(subreddit,))
        thread.start()
        threads.append(thread)

    # Esperar a que todos los hilos terminen
    for thread in threads:
        thread.join()

# Asegurarse de que todos los mensajes se han enviado
producer.flush()
