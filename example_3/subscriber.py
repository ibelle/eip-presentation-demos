from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin

# ========
class Subscriber(ConsumerMixin):
    # #It is essentially a router for incoming messages.
    #fanout delivers messages to all queue's regardless of routing key
    pubsub_exchange = Exchange('pubsub', 'fanout', durable=True)
    #Now Lets declare the task queue where we will pull log messages.
    #This is an anonymous per connection queue as specified with 'exclusive'
    log_queue = Queue(exchange=pubsub_exchange, exclusive=True)

    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.log_queue,
                         callbacks=[self.process_task])]

    def process_task(self, body, message):
        print " [x] %r" % (body,)


if __name__ == '__main__':
    from kombu import Connection
    from kombu.utils.debug import setup_logging
    # setup root logger
    setup_logging(loglevel='INFO', loggers=[''])

    with Connection('amqp://guest:guest@localhost//') as conn:
        try:
            worker = Subscriber(conn)
            print ' [*] Waiting for logs. To exit press CTRL+C'
            worker.run()  #Start consuming messages
        except KeyboardInterrupt:
            print('Connection Terminated!')
