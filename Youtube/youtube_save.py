import os
import json
import datetime
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import time

# Cargar las variables de entorno desde youtube.env
load_dotenv('youtube.env')

# Configuración del consumidor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'youtube-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['youtube-videos'])

# Crear la carpeta 'data' si no existe
if not os.path.exists('data'):
    os.makedirs('data')

# Guardar la hora de inicio
hora_inicio = datetime.datetime.now()

# Función para consumir mensajes de Kafka y guardarlos en un archivo JSON periódicamente
def consumir_y_guardar(duracion_horas=1, intervalo_minutos=5):
    videos_recibidos = []  # Lista para almacenar todos los videos recibidos

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
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Error: {msg.error()}")
                        break

                # Procesar el mensaje
                mensaje = json.loads(msg.value().decode('utf-8'))

                # Verificar si es un mensaje de finalización
                if mensaje.get('status') == 'finalizado':
                    print("Mensaje de finalización recibido. Terminando el consumo.")
                    break

                # Agregar el video a la lista
                videos_recibidos.append(mensaje)
                print(f"Video recibido: {mensaje['title']}")

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
        with open(f'data/youtube_videos_{rango_horas}.json', 'w') as f:
            json.dump(videos_recibidos, f, indent=4)

        # Sobreescribir o agregar al archivo 'youtube.json' para mantener un registro continuo
        youtube_json_path = 'data/youtube.json'
        
        # Leer los datos existentes de 'youtube.json' si el archivo ya existe
        if os.path.exists(youtube_json_path) and os.path.getsize(youtube_json_path) > 0:
            try:
                with open(youtube_json_path, 'r') as youtube_file:
                    existing_data = json.load(youtube_file)
            except json.JSONDecodeError:
                print("Error al decodificar el archivo youtube.json. Inicializando datos como lista vacía.")
                existing_data = []
        else:
            existing_data = []

        # Añadir los nuevos datos al archivo existente
        existing_data.extend(videos_recibidos)

        # Guardar todos los datos nuevamente en 'youtube.json'
        with open(youtube_json_path, 'w') as youtube_file:
            json.dump(existing_data, youtube_file, indent=4)

        consumer.close()
        print(f"Datos de videos guardados en data/youtube_videos_{rango_horas}.json")
        print(f"Datos acumulados guardados en data/youtube.json")

# Ejecutar la función de consumo con escucha cada 5 minutos durante 1 hora
if __name__ == "__main__":
    consumir_y_guardar(duracion_horas=1, intervalo_minutos=5)
