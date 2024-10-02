import time
import praw
import json
import os
from dotenv import load_dotenv
from confluent_kafka import Producer
from datetime import datetime

# Cargar las variables de entorno desde reddit.env
load_dotenv('reddit.env')

# Configuración del productor de Kafka
conf = {'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS')}
producer = Producer(conf)

# Lista de palabras clave relacionadas con tecnologías
palabras_clave_tecnologias = ['AI', 'machine learning', 'big data', 'cloud', 'blockchain', 'data science', 'python']

# Función para enviar los datos a Kafka
def send_to_kafka(topic, data):
    try:
        producer.produce(topic, value=json.dumps(data).encode('utf-8'))
        producer.flush()
    except Exception as e:
        print(f"Error al enviar datos a Kafka: {e}")

# Función para verificar si el título contiene alguna palabra clave relacionada con tecnología
def contiene_palabra_clave(titulo):
    titulo_lower = titulo.lower()
    for palabra in palabras_clave_tecnologias:
        if palabra.lower() in titulo_lower:
            return palabra  # Devuelve la palabra clave que coincide
    return None  # Si no coincide ninguna palabra clave

# Función para obtener los posts más recientes de cada subreddit que contengan palabras clave de tecnología
def obtener_ultimos_posts(subreddit_name, max_posts=5):
    try:
        reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        subreddit = reddit.subreddit(subreddit_name)
        posts_data = []

        # Buscar los posts más recientes y filtrarlos por las palabras clave de tecnología
        for submission in subreddit.new(limit=50):  # Buscar hasta 50 posts para encontrar los relevantes
            palabra_clave_encontrada = contiene_palabra_clave(submission.title)
            if palabra_clave_encontrada:
                post_data = {
                    'post_id': submission.id,
                    'title': submission.title,
                    'author': str(submission.author),
                    'created': submission.created_utc,
                    'num_comments': submission.num_comments,
                    'upvotes': submission.score,
                    'upvote_ratio': submission.upvote_ratio,  # Ratio de upvotes a downvotes
                    'num_comments_inicial': submission.num_comments,  # Número de comentarios inicial
                    'upvotes_inicial': submission.score,  # Número de upvotes inicial
                    'url': submission.url,
                    'permalink': submission.permalink,  # Enlace relativo dentro de Reddit
                    'selftext': submission.selftext,  # Texto del post
                    'subreddit': str(submission.subreddit),
                    'subreddit_subscribers': submission.subreddit_subscribers,  # Suscriptores del subreddit
                    'over_18': submission.over_18,  # Si es NSFW
                    'is_video': submission.is_video,  # Si contiene un video
                    'media': submission.media,  # Información de medios (si tiene)
                    'stickied': submission.stickied,  # Si está "pegado" en la parte superior del subreddit
                    'num_crossposts': submission.num_crossposts,  # Número de crossposts
                    'tendencia': palabra_clave_encontrada,  # Palabra clave que coincidió
                    'fecha_hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                posts_data.append(post_data)
                send_to_kafka('reddit_data', post_data)  # Enviar a Kafka
                print(f"Post enviado: {submission.title} (Palabra clave: {palabra_clave_encontrada})")
                
                if len(posts_data) >= max_posts:
                    break  # Terminar cuando encontramos el número máximo de posts

    except Exception as e:
        print(f"Error al obtener posts del subreddit {subreddit_name}: {e}")
    
    return posts_data

# Función para actualizar los posts ya capturados cada cierto intervalo de tiempo
def actualizar_posts_capturados(posts_capturados):
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )

    # Revisar cada post capturado previamente
    for post in posts_capturados:
        submission = reddit.submission(id=post['post_id'])
        
        # Actualizar los datos cambiantes como comentarios, votos, etc.
        post['num_comments_actualizados'] = submission.num_comments
        post['upvotes_actualizados'] = submission.score
        post['upvote_ratio_actualizado'] = submission.upvote_ratio
        post['fecha_actualizacion'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Enviar la actualización a Kafka
        send_to_kafka('reddit_data', post)
        print(f"Post actualizado: {post['title']} | Fecha de actualización: {post['fecha_actualizacion']}")

# Función principal para capturar y monitorear cambios en los posts
def ejecutar_extraccion_y_monitoreo(subreddits, intervalo_minutos=5, duracion_horas=1):
    tiempo_total = duracion_horas * 3600  # Convertir horas a segundos
    tiempo_inicial = time.time()

    posts_capturados = []  # Lista para almacenar los posts capturados inicialmente

    # Capturar posts al inicio
    for subreddit in subreddits:
        print(f"Consultando el subreddit: {subreddit}")
        posts_capturados.extend(obtener_ultimos_posts(subreddit, max_posts=10))

    print(f"Post IDs capturados para seguimiento: {[post['post_id'] for post in posts_capturados]}")

    # Monitorear cambios en los posts cada intervalo de tiempo
    while (time.time() - tiempo_inicial) < tiempo_total:
        time.sleep(intervalo_minutos * 60)  # Esperar el intervalo de tiempo definido
        print(f"Monitoreando cambios en los posts a las {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        actualizar_posts_capturados(posts_capturados)

    print("Proceso de monitoreo finalizado.")

# Ejecutar el productor
if __name__ == "__main__":
    subreddits = input("Introduce los subreddits separados por comas (máx 10): ").split(',')
    subreddits = [sub.strip() for sub in subreddits if sub.strip() != ''][:10]  # Limitar a 10 subreddits

    ejecutar_extraccion_y_monitoreo(subreddits, intervalo_minutos=5, duracion_horas=1)
