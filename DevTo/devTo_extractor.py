import requests
import time
import json
from confluent_kafka import Producer
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde devto.env
load_dotenv('devto.env')

# Configuración de Kafka Producer
producer = Producer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS')})

# Función para enviar los datos a Kafka
def enviar_a_kafka(topic, mensaje):
    producer.produce(topic, json.dumps(mensaje).encode('utf-8'))
    producer.flush()

# Función para extraer artículos de Dev.to en tiempo real por tag
def extraer_articulos_devto(tag):
    url = f"https://dev.to/api/articles?tag={tag}"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error al obtener artículos para el tag '{tag}': {response.status_code}")
        return []

# Productor que consulta Dev.to cada 5 minutos durante un intervalo de tiempo para varios tags
def ejecutar_extraccion_devto(tags, duracion_horas=1, intervalo_minutos=5):
    tiempo_total = duracion_horas * 3600  # Convertir horas a segundos
    intervalo_segundos = intervalo_minutos * 60  # Convertir minutos a segundos

    tiempo_inicial = time.time()
    contador = 0
    while (time.time() - tiempo_inicial) < tiempo_total:
        for tag in tags:
            # Obtener artículos más recientes por cada tag
            articulos = extraer_articulos_devto(tag)

            # Enviar los datos de los artículos a Kafka
            for articulo in articulos:
                contador += 1
                datos_articulo = {
                    'id': articulo['id'],
                    'title': articulo['title'],
                    'url': articulo['url'],
                    'published_at': articulo['published_at'],
                    'user': articulo['user']['username'],
                    'comments_count': articulo['comments_count'],
                    'positive_reactions_count': articulo['positive_reactions_count'],
                    'tag': tag,
                    'consulta_numero': contador,
                    'timestamp': time.time()
                }
                enviar_a_kafka('devto-articles', datos_articulo)
                print(f"Artículo enviado: {datos_articulo['title']} (Consulta {contador}) del tag: {tag}")

        # Esperar antes de la siguiente consulta a Dev.to
        time.sleep(intervalo_segundos)

    print("Proceso de extracción finalizado.")

# Ejecutar el productor
if __name__ == "__main__":
    tags_input = input("Introduce los tags para buscar artículos en Dev.to, separados por comas (ej. technology,python): ")
    tags = [tag.strip() for tag in tags_input.split(',')]  # Convertir los tags en una lista
    ejecutar_extraccion_devto(tags=tags, duracion_horas=1, intervalo_minutos=5)
