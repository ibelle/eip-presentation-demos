from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin


# Simple Receiver
#EXAMPLE: python example_1/receive.py
#==========
class BasicReceiver(ConsumerMixin):
    #It is essentially a router for incoming messages
    default_exchange = Exchange('', 'direct', durable=True)
    #Now Lets declare the task queue where we will pull work messages
    hello_queue = Queue('Hello', exchange=default_exchange, routing_key='Hello')

    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.hello_queue,
                         callbacks=[self.process_task],
                         no_ack=True)]

    def process_task(self, body, message):
        print " [x] Received %r" % (body,)


if __name__ == '__main__':
    from kombu import Connection
    from kombu.utils.debug import setup_logging
    # setup root logger
    setup_logging(loglevel='INFO', loggers=[''])
    with Connection('amqp://guest:guest@localhost//') as conn:
        try:
            worker = BasicReceiver(conn)
            print '[*] Waiting for messages. To exit press CTRL+C'
            worker.run()
        except KeyboardInterrupt:
            print('Connection Terminated!')