import time
from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin

#EXAMPLE:python example_2/worker.py
class Worker(ConsumerMixin):
    #It is essentially a router for incoming messages
    default_exchange = Exchange('', 'direct', durable=True)
    #Now Lets declare the task queue where we will pull work messages
    task_queue = Queue('Task_Queue', exchange=default_exchange)

    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, Consumer, channel):
        consumer = Consumer(queues=self.task_queue,
                         callbacks=[self.process_task])
        consumer.qos(prefetch_count=1)
        return [consumer]

    def process_task(self, body, message):
        print " [x] Received %r" % (body,)
        work = body['work']
        time.sleep(work.count('.'))
        print " [x] Done"
        message.ack()


if __name__ == '__main__':
    from kombu import Connection
    from kombu.utils.debug import setup_logging
    # setup root logger
    setup_logging(loglevel='INFO', loggers=[''])
    with Connection('amqp://guest:guest@localhost//') as conn:
        try:
            worker = Worker(conn)
            print '[*] Waiting for messages. To exit press CTRL+C'
            worker.run()
        except KeyboardInterrupt:
            print('Connection Terminated!')