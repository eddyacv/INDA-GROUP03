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

# Función para buscar videos recientes o populares relacionados con una palabra clave
# Se consultan los primeros 25 resultados o el valor definido en max_results
def buscar_videos_palabra_clave(palabra_clave, tipo_busqueda='recientes', max_results=25):
    order = 'date' if tipo_busqueda == 'recientes' else 'viewCount'  # Para "populares" ordenar por vistas
    request = youtube.search().list(
        q=palabra_clave,          # Palabra clave de búsqueda
        part="snippet",           # Incluir los detalles del video (título, canal, fecha)
        type="video",             # Filtrar por tipo de contenido (videos)
        maxResults=max_results,   # Número máximo de resultados a obtener
        order=order               # Ordenar por fecha o por cantidad de vistas (dependiendo de tipo_busqueda)
    )
    response = request.execute()
    videos = []
    for item in response['items']:
        videos.append({
            'video_id': item['id']['videoId'],           # ID del video
            'title': item['snippet']['title'],           # Título del video
            'channel': item['snippet']['channelTitle'],  # Canal del video
            'publishedAt': item['snippet']['publishedAt'], # Fecha de publicación
            'palabra_clave': palabra_clave,               # Incluir la palabra clave utilizada en la búsqueda
            'tipo_busqueda': tipo_busqueda                # Indicar si es 'recientes' o 'populares'
        })
    return videos

# Función para obtener estadísticas de un video en YouTube a partir de su ID
def obtener_datos_video(video_id, palabra_clave, tipo_busqueda):
    request_stats = youtube.videos().list(
        part="snippet,statistics",  # Incluir tanto los detalles como las estadísticas
        id=video_id                 # Especificar el ID del video
    )
    response_stats = request_stats.execute()

    if not response_stats['items']:
        return None

    video_data = response_stats['items'][0]
    video_info = {
        'title': video_data['snippet']['title'],         # Título del video
        'viewCount': int(video_data['statistics'].get('viewCount', 0)),  # Cantidad de vistas
        'likeCount': int(video_data['statistics'].get('likeCount', 0)),  # Cantidad de likes
        'commentCount': int(video_data['statistics'].get('commentCount', 0)),  # Cantidad de comentarios
        'url': f"https://www.youtube.com/watch?v={video_id}",  # URL del video
        'fecha_consulta': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # Fecha y hora de consulta actual
        'palabra_clave': palabra_clave,  # Palabra clave utilizada para buscar el video 
        'tipo_busqueda': tipo_busqueda   # Indicar si es un video reciente o popular
    }

    return video_info

# Función para enviar los datos obtenidos a Kafka
def enviar_a_kafka(topic, mensaje):
    producer.produce(topic, json.dumps(mensaje).encode('utf-8'))
    producer.flush()

# Función principal para ejecutar el proceso de extracción de videos
def ejecutar_extraccion_youtube(palabras_clave, duracion_horas=1, intervalo_minutos=5):
    tiempo_total = duracion_horas * 3600  # Convertir horas a segundos
    intervalo_segundos = intervalo_minutos * 60  # Convertir minutos a segundos

    # Guardar la fecha y hora de inicio de la búsqueda
    fecha_inicio = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Búsqueda iniciada en: {fecha_inicio}")
    
    tiempo_inicial = time.time()  # Registrar el tiempo de inicio
    iteracion = 0  # Contador de iteraciones
    consulta_numero = 1  # Contador de consultas

    while (time.time() - tiempo_inicial) < tiempo_total:
        iteracion += 1
        fecha_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Recorrer todas las palabras clave proporcionadas
        for palabra_clave in palabras_clave:
            print(f"Consultando videos recientes para la palabra clave: {palabra_clave}")
            videos_recientes = buscar_videos_palabra_clave(palabra_clave, tipo_busqueda='recientes')

            print(f"Consultando videos populares para la palabra clave: {palabra_clave}")
            videos_populares = buscar_videos_palabra_clave(palabra_clave, tipo_busqueda='populares')

            # Combinar videos recientes y populares
            todos_los_videos = videos_recientes + videos_populares

            for video in todos_los_videos:
                datos_video = obtener_datos_video(video['video_id'], palabra_clave, video['tipo_busqueda'])
                if datos_video:
                    datos_video['timestamp'] = fecha_actualizacion
                    datos_video['iteracion'] = iteracion
                    datos_video['fecha_inicio'] = fecha_inicio
                    datos_video['fecha_actualizacion'] = fecha_actualizacion
                    datos_video['consulta_numero'] = consulta_numero  # Añadir el número de consulta

                    # Enviar los datos a Kafka
                    enviar_a_kafka('youtube-videos', datos_video)
                    print(f"Video enviado: {datos_video['title']} | Tipo: {datos_video['tipo_busqueda']} | Iteración: {iteracion} | Consulta: {consulta_numero} | Fecha Actualización: {fecha_actualizacion}")

        consulta_numero += 1  # Incrementar el número de consulta después de cada iteración
        time.sleep(intervalo_segundos)

    # Enviar mensaje de finalización a Kafka
    mensaje_finalizacion = {
        "status": "finalizado",
        "mensaje": f"Se han enviado todos los videos durante el período",
        "fecha_inicio": fecha_inicio,
        "fecha_finalizacion": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "consulta_numero": consulta_numero - 1
    }
    enviar_a_kafka('youtube-videos', mensaje_finalizacion)
    print("Todos los videos han sido enviados. Mensaje de finalización enviado.")

# Ejecutar el productor
if __name__ == "__main__":
    palabras_clave = input("Introduce las palabras clave para buscar videos (separadas por comas): ").split(',')
    palabras_clave = [palabra.strip() for palabra in palabras_clave if palabra.strip() != '']  # Limpiar las palabras clave
    ejecutar_extraccion_youtube(palabras_clave, duracion_horas=1, intervalo_minutos=5)
