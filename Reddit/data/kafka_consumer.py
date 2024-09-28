from confluent_kafka import Consumer

# Configuración del consumidor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)

# Suscribirse a múltiples tópicos
consumer.subscribe(['reddit_data'])#Agregar Topicos (API) 

# Leer mensajes de los tópicos
while True:
    msg = consumer.poll(1.0)  # Espera hasta 1 segundo para recibir un mensaje
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue

    # Procesar el mensaje recibido
    print(f"Mensaje recibido de {msg.topic()}: {msg.value().decode('utf-8')}")

# Cerrar el consumidor al finalizar
consumer.close()
