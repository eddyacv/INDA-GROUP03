import os
import json
import time
from googleapiclient.discovery import build
from confluent_kafka import Producer
from dotenv import load_dotenv
from datetime import datetime

# Cargar las variables de entorno desde youtube.env
load_dotenv('youtube.env')

# Obtener la clave API desde las variables de entorno
api_key = os.getenv('YOUTUBE_API_KEY')

# Inicializar cliente de YouTube API
youtube = build('youtube', 'v3', developerKey=api_key)

# Configuración de Kafka Producer
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Función para buscar los primeros 50 videos relacionados con una palabra clave
def buscar_videos_palabra_clave(palabra_clave, max_results=10):
    request = youtube.search().list(
        q=palabra_clave,
        part="snippet",
        type="video",
        maxResults=max_results,
        order="date"  # Ordenar por los más recientes
    )
    response = request.execute()

    # Obtener los IDs y detalles de los videos
    videos = []
    for item in response['items']:
        videos.append({
            'video_id': item['id']['videoId'],
            'title': item['snippet']['title'],
            'channel': item['snippet']['channelTitle'],
            'publishedAt': item['snippet']['publishedAt']
        })
    return videos

# Función para obtener estadísticas y comentarios del video
def obtener_datos_video(video_id):
    # Obtener estadísticas del video
    request_stats = youtube.videos().list(
        part="snippet,statistics",
        id=video_id
    )
    response_stats = request_stats.execute()
    
    if not response_stats['items']:
        return None

    video_data = response_stats['items'][0]
    video_info = {
        'video_id': video_id,
        'title': video_data['snippet']['title'],
        'channel': video_data['snippet']['channelTitle'],
        'publishedAt': video_data['snippet']['publishedAt'],
        'viewCount': int(video_data['statistics'].get('viewCount', 0)),
        'likeCount': int(video_data['statistics'].get('likeCount', 0)),
        'dislikeCount': int(video_data['statistics'].get('dislikeCount', 0)) if 'dislikeCount' in video_data['statistics'] else None,
        'commentCount': int(video_data['statistics'].get('commentCount', 0)),
        'fecha_consulta': datetime.now().isoformat()
    }

    # Obtener los comentarios del video
    request_comments = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=10  # Puedes ajustar este valor
    )
    response_comments = request_comments.execute()

    comentarios = []
    for item in response_comments.get('items', []):
        comentarios.append({
            'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            'text': item['snippet']['topLevelComment']['snippet']['textOriginal'],
            'likeCount': item['snippet']['topLevelComment']['snippet']['likeCount'],
            'publishedAt': item['snippet']['topLevelComment']['snippet']['publishedAt']
        })

    video_info['comments'] = comentarios
    return video_info

# Función para enviar los datos de los videos a Kafka
def enviar_a_kafka(topic, mensaje):
    producer.produce(topic, json.dumps(mensaje).encode('utf-8'))
    producer.flush()

# Productor que consulta YouTube cada 5 minutos durante un intervalo de tiempo
def ejecutar_extraccion_youtube(palabra_clave, duracion_horas=1, intervalo_minutos=5):
    tiempo_total = duracion_horas * 3600  # Convertir horas a segundos
    intervalo_segundos = intervalo_minutos * 60  # Convertir minutos a segundos

    tiempo_inicial = time.time()
    iteracion = 0  # Contador de iteraciones

    while (time.time() - tiempo_inicial) < tiempo_total:
        iteracion += 1
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Obtener timestamp actual

        # Buscar los primeros 50 videos relacionados con la palabra clave
        videos = buscar_videos_palabra_clave(palabra_clave)

        # Obtener y enviar los datos completos de los videos al tópico de Kafka
        for video in videos:
            datos_video = obtener_datos_video(video['video_id'])
            if datos_video:
                # Añadir timestamp y palabra clave al mensaje
                datos_video['timestamp'] = timestamp
                datos_video['palabra_clave'] = palabra_clave
                datos_video['iteracion'] = iteracion

                enviar_a_kafka('youtube-videos', datos_video)
                print(f"Video enviado: {datos_video['title']} | Iteración: {iteracion} | Timestamp: {timestamp}")

        # Esperar antes de la siguiente consulta a YouTube
        time.sleep(intervalo_segundos)

    # Enviar mensaje de finalización para indicar que se ha completado la búsqueda
    mensaje_finalizacion = {"status": "finalizado", "mensaje": f"Se han enviado todos los videos durante el período para '{palabra_clave}'", "timestamp": timestamp}
    enviar_a_kafka('youtube-videos', mensaje_finalizacion)
    print("Todos los videos han sido enviados. Mensaje de finalización enviado.")

# Ejecutar el productor con intervalo de 5 minutos durante 1 hora
if __name__ == "__main__":
    palabra_clave = input("Introduce la palabra clave para buscar videos (ej. BigData): ")
    ejecutar_extraccion_youtube(palabra_clave, duracion_horas=1, intervalo_minutos=5)
