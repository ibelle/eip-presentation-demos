import sys
from kombu import Exchange, Queue, binding
from kombu.mixins import ConsumerMixin


# Content Based Routing
#EXAMPLE:  python example_4/log_receiver.py info
# python example_4/log_receiver.py warning error
#========
class LogReceiver(ConsumerMixin):
    #It is essentially a router for incoming messages
    direct_log_exchange = Exchange('direct_logs', 'direct', durable=True)
    log_queue = None

    def __init__(self, connection):
        self.connection = connection

    def init_queue(self, severity_bindings):
        bindings = [
            binding(exchange=self.direct_log_exchange, routing_key=severity)
            for severity in severity_bindings
        ]
        self.log_queue = Queue(exchange=self.direct_log_exchange, exclusive=True, bindings=bindings)

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.log_queue,
                         callbacks=[self.process_task],
                         no_ack=True)]

    def process_task(self, body, message):
        print " [x] %r:%r" % (message.delivery_info['routing_key'], body,)


if __name__ == '__main__':
    from kombu import Connection
    from kombu.utils.debug import setup_logging
    # setup root logger
    setup_logging(loglevel='INFO', loggers=[''])
    severities = sys.argv[1:]
    if not severities:
        print >> sys.stderr, "Usage: %s [info] [warning] [error]" % \
                             (sys.argv[0],)
        sys.exit(1)

    with Connection('amqp://guest:guest@localhost//') as conn:
        try:
            worker = LogReceiver(conn)
            worker.init_queue(severities)
            print ' [*] Waiting for logs (%s) . To exit press CTRL+C' % (severities,)
            worker.run()
        except KeyboardInterrupt:
            print('Connection Terminated!')