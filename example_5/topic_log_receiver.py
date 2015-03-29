import sys
from kombu import Exchange, Queue, binding
from kombu.mixins import ConsumerMixin


#Content Based Routing
#EXAMPLE:  python example_4/log_receiver.py info
#python example_5/topic_log_receiver.py tech.error ux.info
#python example_5/topic_log_receiver.py tech.warn ux.error
class TopicLogReceiver(ConsumerMixin):
    #It is essentially a router for incoming messages
    topic_log_exchange = Exchange('topic_logs', 'topic', durable=True)
    log_queue = None

    def __init__(self, connection):
        self.connection = connection

    def init_queue(self, topic_bindings):
        #bind topics to queue
        bindings = []
        for topic in topic_bindings:
            bindings.append(binding(exchange=self.topic_log_exchange, routing_key=topic))
        self.log_queue = Queue(exchange=self.topic_log_exchange, exclusive=True, bindings=bindings)

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.log_queue,
                         callbacks=[self.process_task],
                         no_ack=True)]

    def process_task(self, body, message):
        print " [x] %s:%r" % (message.delivery_info['routing_key'], body,)

if __name__ == '__main__':
    from kombu import Connection
    from kombu.utils.debug import setup_logging
    # setup root logger
    setup_logging(loglevel='INFO', loggers=[''])
    topics = sys.argv[1:]
    if not topics:
        print >> sys.stderr, "Usage: %s [info] [warning] [error]" % \
                         (sys.argv[0],)
        sys.exit(1)

    with Connection('amqp://guest:guest@localhost//') as conn:
        try:
            worker = TopicLogReceiver(conn)
            worker.init_queue(topics)
            print ' [*] Waiting for logs (%s) . To exit press CTRL+C' % (topics,)
            worker.run()
        except KeyboardInterrupt:
            print('Connection Terminated!')
