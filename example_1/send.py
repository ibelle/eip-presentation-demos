from kombu import Exchange, Queue


#======
class BasicSender(object):
    #Exchange is a class needed by RabbitMQ so that you can actually send to the queue.
    #It is essentially a router for incoming messages
    #Direct will deliver messages to a particular queue by matching routing key of message to routing key of the que
    default_exchange = Exchange('', 'direct', durable=True)
     #Now Lets declare the the queue where we will send messages
    hello_queue = Queue('Hello', exchange=default_exchange, routing_key='Hello')
    _producer = None

    #Build the sender with a simple Point-to-Point Channel endpoint
    def __init__(self, connection):
        self._producer = connection.Producer(serializer='json')

    def publish_message(self, message='Hello World!'):
        self._producer.publish('Hello World!',
                        content_type='text/plain',
                        content_encoding='utf-8',
                        exchange=self.default_exchange,
                        routing_key='Hello',
                        declare=[self.hello_queue])
        print " [x] Sent 'Hello World!'"

if __name__ == '__main__':
    from kombu import Connection
    from kombu.utils.debug import setup_logging
    # setup root logger
    setup_logging(loglevel='INFO', loggers=[''])

    #Declare connection to Message Server
    with Connection('amqp://guest:guest@localhost//') as conn:
        producer = BasicSender(conn)
        producer.publish_message()
