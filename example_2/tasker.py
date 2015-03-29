import sys, random
from kombu import Exchange, Queue

# Task Creator (persistent messages, with ack) - Simple Work/Task Queue
#EXAMPLE: python example_2/tasker.py Work10................
#==
class Tasker(object):
    #It is essentially a router for incoming messages
    default_exchange = Exchange('', 'direct', durable=True)
    #Now Lets declare the task queue where we will send work messages
    task_queue = Queue('Task_Queue', exchange=default_exchange, routing_key='Task_Queue')
    _producer = None

    def __init__(self, connection):
        self._producer = connection.Producer(serializer='json')

    def publish_message(self, message='Logger Message!!'):
        self._producer.publish(message,
                      exchange=self.default_exchange,
                      routing_key='Task_Queue',
                      declare=[self.task_queue],
                      delivery_mode=Exchange.PERSISTENT_DELIVERY_MODE)
        print " [x] Sent %r" % (message,)


if __name__ == '__main__':
    from kombu import Connection
    from kombu.utils.debug import setup_logging
    # setup root logger
    setup_logging(loglevel='INFO', loggers=[''])

    #Messages
   #Let's Build Task Messages from Command Line Args OR just use a default body
    message_body = ' '.join(sys.argv[1:]) or "Hello World..."
    # And ensure we use a more complex binary type
    msg = {'task': 'default tasks',
               'work': '%s' % (message_body,),
               'id' : random.randint(1, 999999999) # Fake Message ID
               }

    #Declare connection to Message Server
    with Connection('amqp://guest:guest@localhost//') as conn:
        producer = Tasker(conn)
        producer.publish_message(msg)
