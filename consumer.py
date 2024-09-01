import concurrent.futures
import time
from kombu.mixins import ConsumerProducerMixin
from kombu import Connection, Exchange, Queue
import logging
from amqp.exceptions import PreconditionFailed 

RABBITMQ_URL = "amqp://admin:admin@188.166.234.51:5672//"  # Replace with your RabbitMQ URL
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Worker(ConsumerProducerMixin):
    def __init__(self, broker_url, queue_name, exchange_name, worker_name):
        self.broker_url = broker_url
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.worker_name = worker_name

    def get_consumers(self, consumer, channel):
        exchange = Exchange(self.exchange_name, type='direct')
        queue = Queue(self.queue_name, exchange, routing_key=self.queue_name)
        
        return [consumer(
            queues=[queue],
            accept=['text/plain'],
            callbacks=[self.process_message],
            prefetch_count=2
        )]

    def process_message(self, body, message):
        # Add your message processing logic here
        logger.info(f"{self.worker_name}: Processing message: {body}")
        time.sleep(120)
        message.ack()  # Acknowledge the message

    def on_connection_error(self, exc):
        logger.error(f"{self.worker_name}: Connection error: {exc}")
        time.sleep(1)  # Wait before retrying

    def on_message(self, body, message):
        try:
            self.process_message(body, message)
        except Exception as e:
            logger.error(f"{self.worker_name}: Error processing message: {e}")
            message.reject()  # Reject the message if processing fails

    def run(self):
        with Connection(self.broker_url) as conn:
            self.connection = conn
            super().run()
        

def start_worker(worker_name):
    logger.info(f"{worker_name} started")
    worker = Worker(RABBITMQ_URL, 'test_queue', 'test_exchange', worker_name)
    worker.run()

def shutdown_workers(executor):
    logger.info("Shutting down workers...")
    executor.shutdown(wait=True)
    logger.info("All workers have been shut down.")
    exit(0)

if __name__ == '__main__':
    try:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for i in range(1, 3):
                logger.info(f"Starting worker {i}")
                executor.submit(
                    start_worker,
                    f'worker-{i}',
                )
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt, stopping workers")
        shutdown_workers(executor)