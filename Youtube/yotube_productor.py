import os
from dotenv import load_dotenv
from confluent_kafka import Producer
from googleapiclient.discovery import build
import json

# Cargar las variables de entorno desde youtube.env
load_dotenv('youtube.env')

# Obtener la clave API desde las variables de entorno
api_key = os.getenv('YOUTUBE_API_KEY')

# Inicializar cliente de YouTube API
youtube = build('youtube', 'v3', developerKey=api_key)

# Configuración de Kafka Producer
producer = Producer({'bootstrap.servers': 'localhost:9092'})  # Cambia esto según tu configuración de Kafka

# Función para obtener detalles de un video de YouTube
def obtener_datos_video(video_id):
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
    response = request.execute()
    return response

# Función para enviar los datos a Kafka
def enviar_a_kafka(topic, mensaje):
    producer.produce(topic, json.dumps(mensaje).encode('utf-8'))
    producer.flush()

# ID de video de YouTube
video_id = 'VIDEO_ID'  # Reemplaza con el ID de un video real

# Obtener los datos del video
datos_video = obtener_datos_video(video_id)

# Enviar los datos del video al tópico de Kafka
enviar_a_kafka('youtube-videos', datos_video)

print("Datos enviados a Kafka")
