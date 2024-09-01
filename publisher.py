from kombu import Connection, Exchange, Queue

# Define the RabbitMQ connection parameters
rabbitmq_url = "amqp://admin:admin@188.166.234.51:5672//"  # Replace with your RabbitMQ URL

# Define the exchange and queue
exchange = Exchange("test_exchange", type="direct")
queue = Queue(name="test_queue", exchange=exchange, routing_key="test", type="quorum")

def publish_message(message):
    # Create a connection to RabbitMQ
    with Connection(rabbitmq_url) as connection:
        # Create a producer
        with connection.Producer() as producer:  # Removed 'serializer' to use default 'raw' (plain text)
            # Publish the plain text message to the exchange
            producer.publish(
                message,
                exchange=exchange,
                routing_key="test",
                declare=[queue],
                retry=True
            )
            print(f"Message sent: {message}")

if __name__ == "__main__":
    # Example text message
    for i in range(20):
        message = f"Hello, RabbitMQ {i}!"
        publish_message(message)
