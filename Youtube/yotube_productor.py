import os
import json
import time
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from confluent_kafka import Producer
from dotenv import load_dotenv
from datetime import datetime

# Cargar las variables de entorno desde youtube.env para obtener las claves de API
load_dotenv('youtube.env')

# Obtener las claves API desde las variables de entorno (puedes agregar más claves aquí)
api_keys = [os.getenv('YOUTUBE_API_KEY1'), os.getenv('YOUTUBE_API_KEY2'),os.getenv('YOUTUBE_API_KEY3')]

# Inicializar la primera clave
api_key_index = 0
current_api_key = api_keys[api_key_index]

# Función para cambiar la clave de API en caso de alcanzar el límite de solicitudes
def cambiar_clave_api():
    global api_key_index, current_api_key, youtube
    api_key_index += 1
    if api_key_index >= len(api_keys):
        print("Se han agotado todas las claves API disponibles. Inténtelo más tarde.")
        return False
    current_api_key = api_keys[api_key_index]
    youtube = build('youtube', 'v3', developerKey=current_api_key)
    print(f"Clave API cambiada a la clave número {api_key_index + 1}")
    return True

# Inicializar cliente de YouTube API con la clave actual
youtube = build('youtube', 'v3', developerKey=current_api_key)

# Configuración de Kafka Producer, con el servidor de Kafka que está en 'localhost'
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Función para buscar videos recientes o populares relacionados con una palabra clave
def buscar_videos_palabra_clave(palabra_clave, tipo_busqueda='recientes', max_results=25):
    global youtube
    order = 'date' if tipo_busqueda == 'recientes' else 'viewCount'
    
    while True:  # Intentar hasta que la solicitud se ejecute o se cambie la clave API
        try:
            request = youtube.search().list(
                q=palabra_clave,
                part="snippet",
                type="video",
                maxResults=max_results,
                order=order
            )
            response = request.execute()
            videos = []
            for item in response['items']:
                videos.append({
                    'video_id': item['id']['videoId'],
                    'title': item['snippet']['title'],
                    'channel': item['snippet']['channelTitle'],
                    'publishedAt': item['snippet']['publishedAt'],
                    'palabra_clave': palabra_clave,
                    'tipo_busqueda': tipo_busqueda
                })
            return videos
        
        except HttpError as e:
            if e.resp.status == 403:
                print("Límite de solicitudes alcanzado con la clave actual.")
                if not cambiar_clave_api():
                    return []  # No hay más claves disponibles
            else:
                raise e  # Si no es un error de límite, elevar el error

# Función para obtener estadísticas de un video en YouTube a partir de su ID
def obtener_datos_video(video_id, palabra_clave, tipo_busqueda):
    global youtube
    while True:
        try:
            request_stats = youtube.videos().list(
                part="snippet,statistics",
                id=video_id
            )
            response_stats = request_stats.execute()

            if not response_stats['items']:
                return None

            video_data = response_stats['items'][0]
            video_info = {
                'title': video_data['snippet']['title'],
                'viewCount': int(video_data['statistics'].get('viewCount', 0)),
                'likeCount': int(video_data['statistics'].get('likeCount', 0)),
                'commentCount': int(video_data['statistics'].get('commentCount', 0)),
                'viewCount_inicial': int(video_data['statistics'].get('viewCount', 0)),
                'likeCount_inicial': int(video_data['statistics'].get('likeCount', 0)),
                'commentCount_inicial': int(video_data['statistics'].get('commentCount', 0)),
                'url': f"https://www.youtube.com/watch?v={video_id}",
                'fecha_consulta': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'palabra_clave': palabra_clave,
                'tipo_busqueda': tipo_busqueda
            }
            return video_info

        except HttpError as e:
            if e.resp.status == 403:
                print("Límite de solicitudes alcanzado con la clave actual.")
                if not cambiar_clave_api():
                    return None  # No hay más claves disponibles
            else:
                raise e

# Función para enviar los datos obtenidos a Kafka
def enviar_a_kafka(topic, mensaje):
    producer.produce(topic, json.dumps(mensaje).encode('utf-8'))
    producer.flush()

# Función principal para ejecutar el proceso de extracción de videos
def ejecutar_extraccion_youtube(palabras_clave, duracion_horas=1, intervalo_minutos=5):
    tiempo_total = duracion_horas * 3600
    intervalo_segundos = intervalo_minutos * 60

    fecha_inicio = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Búsqueda iniciada en: {fecha_inicio}")
    
    tiempo_inicial = time.time()
    iteracion = 0
    consulta_numero = 1

    while (time.time() - tiempo_inicial) < tiempo_total:
        iteracion += 1
        fecha_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for palabra_clave in palabras_clave:
            print(f"Consultando videos recientes para la palabra clave: {palabra_clave}")
            videos_recientes = buscar_videos_palabra_clave(palabra_clave, tipo_busqueda='recientes')

            print(f"Consultando videos populares para la palabra clave: {palabra_clave}")
            videos_populares = buscar_videos_palabra_clave(palabra_clave, tipo_busqueda='populares')

            todos_los_videos = videos_recientes + videos_populares

            for video in todos_los_videos:
                datos_video = obtener_datos_video(video['video_id'], palabra_clave, video['tipo_busqueda'])
                if datos_video:
                    datos_video['timestamp'] = fecha_actualizacion
                    datos_video['iteracion'] = iteracion
                    datos_video['fecha_inicio'] = fecha_inicio
                    datos_video['fecha_actualizacion'] = fecha_actualizacion
                    datos_video['consulta_numero'] = consulta_numero

                    enviar_a_kafka('youtube-videos', datos_video)
                    print(f"Video enviado: {datos_video['title']} | Tipo: {datos_video['tipo_busqueda']} | Iteración: {iteracion} | Consulta: {consulta_numero} | Fecha Actualización: {fecha_actualizacion}")

        consulta_numero += 1
        time.sleep(intervalo_segundos)

    mensaje_finalizacion = {
        "status": "finalizado",
        "mensaje": "Se han enviado todos los videos durante el período",
        "fecha_inicio": fecha_inicio,
        "fecha_finalizacion": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "consulta_numero": consulta_numero - 1
    }
    enviar_a_kafka('youtube-videos', mensaje_finalizacion)
    print("Todos los videos han sido enviados. Mensaje de finalización enviado.")

# Ejecutar el productor
if __name__ == "__main__":
    palabras_clave = input("Introduce las palabras clave para buscar videos (separadas por comas): ").split(',')
    palabras_clave = [palabra.strip() for palabra in palabras_clave if palabra.strip() != '']
    ejecutar_extraccion_youtube(palabras_clave, duracion_horas=1, intervalo_minutos=5)
