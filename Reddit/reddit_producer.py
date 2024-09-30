import time
import praw
import json
import os
from dotenv import load_dotenv
from confluent_kafka import Producer

# Cargar las variables de entorno desde reddit.env
load_dotenv('reddit.env')

# Configuración del productor de Kafka
conf = {'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS')}
producer = Producer(conf)

# Lista de palabras clave relacionadas con tecnologías
palabras_clave_tecnologias = ['AI', 'machine learning', 'big data', 'cloud', 'blockchain', 'data science', 'python', 'javascript', 'devops']

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

# Función para obtener los 5 posts más recientes de cada subreddit que contengan palabras clave de tecnología
def obtener_ultimos_posts(subreddit_name, max_posts=5):
    try:
        reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        subreddit = reddit.subreddit(subreddit_name)
        posts_data = []
        posts_encontrados = 0

        # Buscar los posts más recientes y filtrarlos por las palabras clave de tecnología
        for submission in subreddit.new(limit=50):  # Buscar hasta 50 posts para encontrar al menos 5 relevantes
            palabra_clave_encontrada = contiene_palabra_clave(submission.title)
            if palabra_clave_encontrada:
                post_data = {
                    'post_id': submission.id,
                    'title': submission.title,
                    'author': str(submission.author),
                    'created': submission.created_utc,
                    'num_comments': submission.num_comments,
                    'upvotes': submission.score,
                    'url': submission.url,
                    'subreddit': str(submission.subreddit),
                    'tendencia': palabra_clave_encontrada  # Palabra clave que coincidió
                }
                posts_data.append(post_data)
                send_to_kafka('reddit_data', post_data)  # Enviar a Kafka
                print(f"Post enviado: {submission.title} (Palabra clave: {palabra_clave_encontrada})")
                
                posts_encontrados += 1
                if posts_encontrados >= max_posts:
                    break  # Terminar cuando encontramos el número máximo de posts

    except Exception as e:
        print(f"Error al obtener posts del subreddit {subreddit_name}: {e}")
    
    return posts_data

# Función para consultar la interacción actual de un post
def consultar_interaccion_post(post_id):
    try:
        reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        submission = reddit.submission(id=post_id)
        interaccion = {
            'post_id': post_id,
            'title': submission.title,
            'num_comments': submission.num_comments,
            'upvotes': submission.score,
            'created': submission.created_utc
        }
        return interaccion

    except Exception as e:
        print(f"Error al consultar interacción del post {post_id}: {e}")
        return None

# Función para realizar consultas cada 2 minutos durante una hora
def ejecutar_extraccion_reddit(subreddits, intervalo_minutos=2, duracion_horas=1):
    consultas_realizadas = 0
    posts_ids = []
    tiempo_total = duracion_horas * 3600  # Convertimos horas a segundos
    tiempo_inicial = time.time()

    # Primero obtenemos los últimos posts que contengan palabras clave de cada subreddit
    for subreddit in subreddits:
        print(f"Consultando el subreddit: {subreddit}")
        posts = obtener_ultimos_posts(subreddit, max_posts=5)
        posts_ids.extend([post['post_id'] for post in posts])

    print(f"Post IDs capturados para seguimiento: {posts_ids}")

    # Ahora hacemos consultas repetidas cada 2 minutos para estos posts hasta que se acabe el tiempo
    while (time.time() - tiempo_inicial) < tiempo_total:
        print(f"Consulta #{consultas_realizadas + 1}")

        for post_id in posts_ids:
            interaccion = consultar_interaccion_post(post_id)
            if interaccion:  # Solo envía si la interacción no es None
                send_to_kafka('reddit_data', interaccion)
                print(f"Interacción actualizada enviada para post {post_id}")

        consultas_realizadas += 1
        time.sleep(intervalo_minutos * 60)  # Esperar 2 minutos antes de la siguiente consulta

    print("Proceso de extracción finalizado.")

# Ejecutar el productor
if __name__ == "__main__":

    subreddits = input("Introduce los subreddits separados por comas (máx 10): ").split(',')
    subreddits = [sub.strip() for sub in subreddits if sub.strip() != ''][:10]  # Limitar a 10 subreddits

    ejecutar_extraccion_reddit(subreddits, intervalo_minutos=2, duracion_horas=1)
