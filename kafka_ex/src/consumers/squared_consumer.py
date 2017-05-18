from kafka import KafkaConsumer
from consumers_helpers.math_helpers import square_it
import traceback
from settings import KAFKA_CONNECTION
import multiprocessing
from multiprocessing import Queue
import datetime
import sys
import logging

LOG_FILENAME = 'logging.out'
logging.basicConfig(filename=LOG_FILENAME,
                    level=logging.INFO)
q = Queue()


class Consumer(multiprocessing.Process):
    def __init__(self):
        """
        Custom run method for a multiprocessing process.
        """
        multiprocessing.Process.__init__(self)

    def run(self):
        main_service()


def main():
    """
    Multi-threaded application that has a main_service method inside the consumer that checks a queue for a number and squares it
    :return:
    """

    num_of_consumers = 4
    consumers = [Consumer() for i in xrange(num_of_consumers)]

    for worker in consumers:
        worker.start()

    return consumers


def main_service(q=q):
    '''
    Service that squares things.

    :return:
    '''
    while True:

        # iterate current messages
        msg = q.get()

        # #cast msg to int and square it and print it
        try:
            #print result of square it to stdout
            sys.stdout.write(str(square_it(int(msg.value)))+'\n\r')
            sys.stdout.flush()

        except Exception:
            logging.error(traceback.print_exc())


class SquaredConsumer(object):
    '''
    Squares the things
    '''
    def __init__(self):
        self.local_consumers = main()
        self.last_put = datetime.datetime.now()

    def main(self):
        """
        Main method that consumes from test topic for up to 30 seconds of inactivity
        :return:
        """

        #run forever
        #print fatal tracebacks
        try:
            logging.info('Are processes alive?')
            logging.info([process.is_alive() for process in self.local_consumers])

            while True:
                #create consumer, timeout after 1 second
                consumer = KafkaConsumer('test', bootstrap_servers=KAFKA_CONNECTION, consumer_timeout_ms=1000)
                consumer.subscribe(['test'])

                #iterate current messages
                for msg in consumer:

                    # #cast msg to int and square it
                    try:
                        q.put(msg)
                        logging.info('putting msg: {0}'.format(msg))

                    except Exception:
                        traceback.print_exc()

                    self.last_put = datetime.datetime.now()

                #kill self if no messages in at least 30 seconds.
                if datetime.datetime.now() > self.last_put + datetime.timedelta(seconds=30):
                    logging.info('No messages for at least 30 seconds... Killing program.')

                    # kill program, clean up resources so main thread exits
                    [process.terminate() for process in self.local_consumers]
                    [process.join() for process in self.local_consumers]
                    break

            sys.exit(0)

        except Exception:
            logging.error(traceback.print_exc())


if __name__ == '__main__':
    consumer = SquaredConsumer()
    consumer.main()