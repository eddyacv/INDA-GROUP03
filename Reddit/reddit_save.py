import json
import datetime
import os
from confluent_kafka import Consumer
from dotenv import load_dotenv

# Cargar las variables de entorno desde reddit.env
load_dotenv('reddit.env')

# Configuración del consumidor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['reddit_data'])

# Lista para almacenar todos los posts y sus comentarios
posts_with_comments = []

# Guardar la hora de inicio
hora_inicio = datetime.datetime.now()

# Crear la carpeta 'data' si no existe
if not os.path.exists('data'):
    os.makedirs('data')

# Leer mensajes del topic
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Procesar el mensaje
        try:
            message_value = json.loads(msg.value().decode('utf-8'))
        except json.JSONDecodeError as e:
            print(f"Error decodificando mensaje JSON: {e}")
            continue

        print(f"Mensaje recibido: {message_value}")

        # Clasificar entre post y comentarios
        if message_value.get('post_id'):
            post_id = message_value.get('post_id')
            # Crear un diccionario con la estructura del post y comentarios vacíos
            post_data = {
                'post_id': post_id,
                'title': message_value.get('title', 'N/A'),
                'author': message_value.get('author', 'N/A'),
                'created': message_value.get('created', 0),
                'subreddit': message_value.get('subreddit', 'N/A'),
                'url': message_value.get('url', 'N/A'),
                'num_comments': message_value.get('num_comments', 0),
                'upvotes': message_value.get('upvotes', 0),
                'tendencia': message_value.get('tendencia', 'null'),
                'comments': []
            }
            # Añadir este post al arreglo si no existe
            if not any(post['post_id'] == post_data['post_id'] for post in posts_with_comments):
                posts_with_comments.append(post_data)

except KeyboardInterrupt:
    print("Proceso interrumpido. Guardando datos...")

finally:
    # Guardar la hora de finalización
    hora_final = datetime.datetime.now()

    # Formato: Mes-Día-Año_HoraInicio-HoraFinal
    rango_horas = f'{hora_inicio.strftime("%m-%d-%Y_%H-%M")}-a-{hora_final.strftime("%H-%M")}'

    # Guardar en la carpeta 'data' con la fecha y hora de inicio y finalización
    with open(f'data/reddit_data_{rango_horas}.json', 'w') as json_file:
        json.dump(posts_with_comments, json_file, indent=4)

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
    for new_post in posts_with_comments:
        if not any(post['post_id'] == new_post['post_id'] for post in existing_data):
            existing_data.append(new_post)

    # Guardar todos los datos nuevamente en 'reddit.json'
    with open(reddit_json_path, 'w') as reddit_file:
        json.dump(existing_data, reddit_file, indent=4)

    print(f"Datos guardados en data/reddit_data_{rango_horas}.json")
    print(f"Datos acumulados guardados en data/reddit.json")

    consumer.close()
