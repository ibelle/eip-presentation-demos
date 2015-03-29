# Topic Based logging and exchanges
import sys

#EXAMPLE:topics=<huge group>.<severity>
#python example_5/topic_logger.py "ux.*" "Logging Test"
from kombu import Exchange


class TopicLogProducer(object):
    #It is essentially a router for incoming messages
    topic_log_exchange = Exchange('topic_logs', 'topic', durable=True)
    _producer = None

    def __init__(self, connection):
        self._producer = connection.Producer(serializer='json')

    def publish_message(self, message='Logger Message!!', topic='anonymous.info'):
        self._producer.publish(message,
                              exchange=self.topic_log_exchange,
                              routing_key=topic)
        print " [x] Sent %r:%r" % (topic, message)


if __name__ == '__main__':
    from kombu import Connection
    from kombu.utils.debug import setup_logging
    # setup root logger
    setup_logging(loglevel='INFO', loggers=[''])

    #Messages
    topic = sys.argv[1] if len(sys.argv) > 1 else 'anonymous.info'
    message = ' '.join(sys.argv[2:]) or 'Logger Message!!'

    #Declare connection to Message Server
    with Connection('amqp://guest:guest@localhost//') as conn:
        producer = TopicLogProducer(conn)
        producer.publish_message(message, topic)


