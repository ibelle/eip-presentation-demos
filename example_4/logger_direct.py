import sys, random

# EXAMPLE: python example_4/logger_direct.py "Message 2"
from kombu import Exchange


class LoggerProducer(object):
    SEVERITIES = ['info', 'warning', 'error']
    direct_log_exchange = Exchange('direct_logs', 'direct', durable=True)
    _producer = None

    def __init__(self, connection):
        self._producer = connection.Producer(serializer='json')

    def publish_message(self, message='Logger Message!!', severity='info'):
        self._producer.publish(message,
                               exchange=self.direct_log_exchange,
                               routing_key=severity)

        print " [x] Sent %r:%r" % (severity, message)


if __name__ == '__main__':
    from kombu import Connection
    from kombu.utils.debug import setup_logging
    # setup root logger
    setup_logging(loglevel='INFO', loggers=[''])

    #Messages
    svty = random.choice(LoggerProducer.SEVERITIES)
    msg = ' '.join(sys.argv[1:]) or 'Hello World!'

    #Declare connection to Message Server
    with Connection('amqp://guest:guest@localhost//') as conn:
        producer = LoggerProducer(conn)
        producer.publish_message(msg, svty)