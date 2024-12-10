import json
import datetime
import os
from confluent_kafka import Consumer
from dotenv import load_dotenv
import time

# Cargar las variables de entorno desde reddit.env
load_dotenv('reddit.env')

# Configuración del consumidor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'reddit-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['reddit_data'])

# Crear la carpeta 'data' si no existe
if not os.path.exists('data'):
    os.makedirs('data')

# Guardar la hora de inicio
hora_inicio = datetime.datetime.now()

# Función para consumir mensajes de Kafka y guardarlos en un archivo JSON periódicamente
def consumir_y_guardar(duracion_horas=1, intervalo_minutos=5):
    posts_recibidos = []  # Lista para almacenar todos los posts recibidos

    tiempo_total = duracion_horas * 3600  # Convertir horas a segundos
    intervalo_segundos = intervalo_minutos * 60  # Convertir minutos a segundos

    tiempo_inicial = time.time()
    try:
        while (time.time() - tiempo_inicial) < tiempo_total:
            print(f"Escuchando mensajes de Kafka... {datetime.datetime.now()}")

            while True:
                msg = consumer.poll(1.0)  # Esperar por mensajes

                if msg is None:
                    continue
                if msg.error():
                    print(f"Error: {msg.error()}")
                    break

                # Procesar el mensaje
                mensaje = json.loads(msg.value().decode('utf-8'))

                # Agregar el post a la lista
                posts_recibidos.append(mensaje)
                print(f"Post recibido: {mensaje['title']}")

            # Pausar por el intervalo definido antes de la próxima escucha
            time.sleep(intervalo_segundos)

    except KeyboardInterrupt:
        print("Proceso interrumpido manualmente. Guardando datos...")

    finally:
        # Guardar la hora de finalización
        hora_final = datetime.datetime.now()

        # Formato: Mes-Día-Año_HoraInicio-HoraFinal
        rango_horas = f'{hora_inicio.strftime("%m-%d-%Y_%H-%M")}-a-{hora_final.strftime("%H-%M")}'

        # Guardar los datos en un archivo JSON con la hora de consulta
        with open(f'data/reddit_posts_{rango_horas}.json', 'w') as f:
            json.dump(posts_recibidos, f, indent=4)

        # Sobreescribir o agregar al archivo 'reddit.json' para mantener un registro continuo
        reddit_json_path = 'data/reddit.json'
        
        # Leer los datos existentes de 'reddit.json' si el archivo ya existe
        if os.path.exists(reddit_json_path) and os.path.getsize(reddit_json_path) > 0:
            try:
                with open(reddit_json_path, 'r') as reddit_file:
                    existing_data = json.load(reddit_file)
            except json.JSONDecodeError:
                print("Error al decodificar el archivo reddit.json. Inicializando datos como lista vacía.")
                existing_data = []
        else:
            existing_data = []

        # Añadir los nuevos datos al archivo existente
        existing_data.extend(posts_recibidos)

        # Guardar todos los datos nuevamente en 'reddit.json'
        with open(reddit_json_path, 'w') as reddit_file:
            json.dump(existing_data, reddit_file, indent=4)

        consumer.close()
        print(f"Datos de posts guardados en data/reddit_posts_{rango_horas}.json")
        print(f"Datos acumulados guardados en data/reddit.json")

# Ejecutar la función de consumo con escucha cada 5 minutos durante 1 hora
if __name__ == "__main__":
    consumir_y_guardar(duracion_horas=1, intervalo_minutos=5)
