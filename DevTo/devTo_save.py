import json
import os
import datetime
from confluent_kafka import Consumer
from dotenv import load_dotenv

# Cargar las variables de entorno desde devto.env
load_dotenv('devto.env')

# Configuración del consumidor de Kafka
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'group.id': 'devto_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['devto-articles'])

# Lista para almacenar los artículos
articulos = []

# Guardar la hora de inicio
hora_inicio = datetime.datetime.now()

# Leer mensajes del tópico y guardar en JSON
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Procesar el mensaje
        message_value = json.loads(msg.value().decode('utf-8'))
        print(f"Artículo recibido: {message_value['title']}")
        
        # Agregar el artículo a la lista
        articulos.append(message_value)

except KeyboardInterrupt:
    print("Proceso interrumpido. Guardando datos...")

finally:
    # Guardar la hora de finalización
    hora_final = datetime.datetime.now()

    # Formato: Mes-Día-Año_HoraInicio-HoraFinal
    rango_horas = f'{hora_inicio.strftime("%m-%d-%Y_%H-%M")}-a-{hora_final.strftime("%H-%M")}'

    # Guardar en un archivo JSON con la fecha y hora de inicio y finalización
    with open(f'data/devto_articles_{rango_horas}.json', 'w') as json_file:
        json.dump(articulos, json_file, indent=4)

    consumer.close()
    print(f"Datos guardados en data/devto_articles_{rango_horas}.json")
