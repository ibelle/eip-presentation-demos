import sys, random
from kombu import Exchange


# Pub/Sub
#EXAMPLE: python example_3/publisher.py
class Publisher(object):
    #It is essentially a router for outgoing messages.
    #fanout delivers messages to all queue's regardless of routing key
    pubsub_exchange = Exchange('pubsub', 'fanout', durable=True)
    _producer = None

    def __init__(self, connection):
        self._producer = connection.Producer(serializer='json')

    def publish_message(self, message='Logger Message!!'):
        self._producer.publish(message,
                               content_type='text/plain',  #Set Content type for Plain Text Messages
                               content_encoding='utf-8',
                               exchange=self.pubsub_exchange,
                               routing_key='')
        print " [x] Sent Log Message::%r" % (message,)


if __name__ == '__main__':
    from kombu import Connection
    from kombu.utils.debug import setup_logging
    # setup root logger
    setup_logging(loglevel='INFO', loggers=[''])

    #Messages
    msg = ' '.join(sys.argv[1:]) or "info-{0}: Hello World!".format(random.randint(1, 999999999))

    #Declare connection to Message Server
    with Connection('amqp://guest:guest@localhost//') as conn:
        producer = Publisher(conn)
        producer.publish_message(msg)
