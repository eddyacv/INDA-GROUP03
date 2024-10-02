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
            
            # Crear un diccionario con todos los campos del post
            post_data = {
                'post_id': post_id,
                'title': message_value.get('title', 'N/A'),
                'author': message_value.get('author', 'N/A'),
                'created': message_value.get('created', 0),
                'num_comments': message_value.get('num_comments', 0),
                'upvotes': message_value.get('upvotes', 0),
                'upvote_ratio': message_value.get('upvote_ratio', 0),  # Ratio de upvotes a downvotes
                'url': message_value.get('url', 'N/A'),
                'permalink': message_value.get('permalink', 'N/A'),  # Enlace relativo dentro de Reddit
                'selftext': message_value.get('selftext', ''),  # Texto del post
                'subreddit': message_value.get('subreddit', 'N/A'),
                'subreddit_subscribers': message_value.get('subreddit_subscribers', 0),  # Suscriptores del subreddit
                'over_18': message_value.get('over_18', False),  # Si es NSFW
                'is_video': message_value.get('is_video', False),  # Si contiene un video
                'media': message_value.get('media', None),  # Información de medios (si tiene)
                'stickied': message_value.get('stickied', False),  # Si está "pegado" en la parte superior del subreddit
                'num_crossposts': message_value.get('num_crossposts', 0),  # Número de crossposts
                'tendencia': message_value.get('tendencia', 'N/A'),  # Palabra clave que coincidió
                'fecha_hora': message_value.get('fecha_hora', 'N/A'),  # Fecha y hora de la consulta
                'fehca_actualizacion': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'num_comments_inicial': message_value.get('num_comments', 0),  # Número de comentarios inicial
                'upvotes_inicial': message_value.get('upvotes', 0),  # Número de upvotes inicial
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
