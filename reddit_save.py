import json
import datetime
from confluent_kafka import Consumer

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
        message_value = json.loads(msg.value().decode('utf-8'))
        print(f"Mensaje recibido: {message_value}")

        # Clasificar entre post y comentarios
        if message_value.get('type') == 'post':
            post_id = message_value.get('post_id')  # Ahora se recibirá correctamente el post_id
            # Crear un diccionario con la estructura del post y comentarios vacíos
            post_data = {
                'post_id': post_id,
                'title': message_value['title'],
                'author': message_value['author'],
                'created': message_value['created'],
                'subreddit': message_value['subreddit'],
                'url': message_value['url'],
                'comments': []
            }
            # Añadir este post al arreglo
            posts_with_comments.append(post_data)
        elif message_value.get('type') == 'comment':
            post_id = message_value.get('post_id')
            # Buscar el post correspondiente para añadir el comentario
            for post in posts_with_comments:
                if post['post_id'] == post_id:
                    post['comments'].append({
                        'comment_id': message_value['comment_id'],
                        'comment_body': message_value['comment_body'],
                        'comment_author': message_value['comment_author'],
                        'created': message_value['created']
                    })

except KeyboardInterrupt:
    print("Proceso interrumpido. Guardando datos...")

finally:
    # Guardar la hora de finalización
    hora_final = datetime.datetime.now()

    # Formato: Mes-Día-Año_HoraInicio-HoraFinal
    rango_horas = f'{hora_inicio.strftime("%m-%d-%Y_%H-%M")}-a-{hora_final.strftime("%H-%M")}'

    # Guardar en un archivo JSON con la fecha y hora de inicio y finalización
    with open(f'reddit_data_{rango_horas}.json', 'w') as json_file:
        json.dump(posts_with_comments, json_file, indent=4)

    consumer.close()
    print(f"Datos guardados en reddit_data_{rango_horas}.json")
