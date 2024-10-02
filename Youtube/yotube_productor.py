import os
import json
import time
from googleapiclient.discovery import build
from confluent_kafka import Producer
from dotenv import load_dotenv
from datetime import datetime

# Cargar las variables de entorno desde youtube.env para obtener las claves de API
load_dotenv('youtube.env')

# Obtener la clave API desde las variables de entorno
api_key = os.getenv('YOUTUBE_API_KEY')

# Inicializar cliente de YouTube API con la clave obtenida
youtube = build('youtube', 'v3', developerKey=api_key)

# Configuración de Kafka Producer, con el servidor de Kafka que está en 'localhost'
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Función para buscar videos relacionados con una palabra clave
# Se consultan los primeros 10 resultados o el valor definido en max_results
def buscar_videos_palabra_clave(palabra_clave, max_results=10):
    # Crear la solicitud para buscar videos en YouTube usando una palabra clave
    request = youtube.search().list(
        q=palabra_clave,          # Palabra clave de búsqueda
        part="snippet",           # Incluir los detalles del video (título, canal, fecha)
        type="video",             # Filtrar por tipo de contenido (videos)
        maxResults=max_results,   # Número máximo de resultados a obtener
        order="date"              # Ordenar por fecha de publicación (más recientes primero)
    )
    # Ejecutar la solicitud y obtener la respuesta en formato JSON
    response = request.execute()

    # Crear una lista para almacenar los detalles de cada video
    videos = []
    for item in response['items']:
        # Extraer los detalles relevantes (ID, título, canal, fecha de publicación)
        videos.append({
            'video_id': item['id']['videoId'],           # ID del video
            'title': item['snippet']['title'],           # Título del video
            'channel': item['snippet']['channelTitle'],  # Canal del video
            'publishedAt': item['snippet']['publishedAt'] # Fecha de publicación
        })
    return videos

# Función para obtener estadísticas de un video en YouTube a partir de su ID
def obtener_datos_video(video_id):
    # Crear la solicitud para obtener estadísticas de un video específico
    request_stats = youtube.videos().list(
        part="snippet,statistics",  # Incluir tanto los detalles como las estadísticas
        id=video_id                 # Especificar el ID del video
    )
    # Ejecutar la solicitud y obtener la respuesta en formato JSON
    response_stats = request_stats.execute()

    # Si el video no existe o no se encontró, devolver None
    if not response_stats['items']:
        return None

    # Extraer la información relevante del video
    video_data = response_stats['items'][0]
    video_info = {
        'title': video_data['snippet']['title'],         # Título del video
        'viewCount': int(video_data['statistics'].get('viewCount', 0)),  # Cantidad de vistas
        'likeCount': int(video_data['statistics'].get('likeCount', 0)),  # Cantidad de likes
        'commentCount': int(video_data['statistics'].get('commentCount', 0)),  # Cantidad de comentarios
        'url': f"https://www.youtube.com/watch?v={video_id}",  # URL del video
        'fecha_consulta': datetime.now().isoformat()  # Fecha y hora de consulta actual
    }

    return video_info

# Función para enviar los datos obtenidos a Kafka
def enviar_a_kafka(topic, mensaje):
    # Codificar los datos en formato JSON y enviarlos a un tópico de Kafka
    producer.produce(topic, json.dumps(mensaje).encode('utf-8'))
    producer.flush()  # Asegurarse de que el mensaje se envíe inmediatamente

# Función principal para ejecutar el proceso de extracción de videos
def ejecutar_extraccion_youtube(palabra_clave, duracion_horas=1, intervalo_minutos=5):
    # Definir el tiempo total que durará el proceso
    tiempo_total = duracion_horas * 3600  # Convertir horas a segundos
    intervalo_segundos = intervalo_minutos * 60  # Convertir minutos a segundos

    tiempo_inicial = time.time()  # Registrar el tiempo de inicio
    iteracion = 0  # Contador de iteraciones

    # Ejecutar el ciclo hasta que pase el tiempo total definido
    while (time.time() - tiempo_inicial) < tiempo_total:
        iteracion += 1  # Incrementar el contador de iteraciones
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Obtener el timestamp actual

        # Buscar los videos relacionados con la palabra clave ingresada
        videos = buscar_videos_palabra_clave(palabra_clave)

        # Obtener y enviar los datos de cada video al tópico de Kafka
        for video in videos:
            datos_video = obtener_datos_video(video['video_id'])
            if datos_video:
                # Añadir el timestamp y la palabra clave a los datos del video
                datos_video['timestamp'] = timestamp
                datos_video['palabra_clave'] = palabra_clave
                datos_video['iteracion'] = iteracion

                # Enviar los datos a Kafka
                enviar_a_kafka('youtube-videos', datos_video)
                print(f"Video enviado: {datos_video['title']} | Iteración: {iteracion} | Timestamp: {timestamp}")

        # Esperar el intervalo definido antes de la siguiente consulta
        time.sleep(intervalo_segundos)

    # Al finalizar, enviar un mensaje de finalización al tópico de Kafka
    mensaje_finalizacion = {"status": "finalizado", "mensaje": f"Se han enviado todos los videos durante el período para '{palabra_clave}'", "timestamp": timestamp}
    enviar_a_kafka('youtube-videos', mensaje_finalizacion)
    print("Todos los videos han sido enviados. Mensaje de finalización enviado.")

# Ejecutar el productor, solicitando al usuario que ingrese una palabra clave
if __name__ == "__main__":
    palabra_clave = input("Introduce la palabra clave para buscar videos (ej. BigData): ")
    ejecutar_extraccion_youtube(palabra_clave, duracion_horas=1, intervalo_minutos=5)

