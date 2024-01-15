from confluent_kafka import Consumer, KafkaException, KafkaError

# Configuración de Kafka para el consumidor
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "science",
    "auto.offset.reset": "earliest",
}


# Función para procesar mensajes
def process_message(msg):
    print(f"Received message: {msg.value().decode('utf-8')}")


# Función para consumir mensajes del topic
def consume_messages(consumer_config):
    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe(["science"])

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error al recibir el mensaje: {msg.error()}")
            else:
                process_message(msg)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    # Consumir mensajes para el sistema independiente 1
    consume_messages(consumer_config)
