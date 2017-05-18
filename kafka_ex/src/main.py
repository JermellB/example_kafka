from consumers.squared_consumer import SquaredConsumer
from producers.squared_producer import SquaredProducer
import multiprocessing


class Consumer(multiprocessing.Process):
    def __init__(self):
        """
        Custom run method for a multiprocessing process.
        """
        multiprocessing.Process.__init__(self)

    def run(self):
        main()


def main():
    '''
    Service that squares things.

    :return:
    '''

    s_consumer = SquaredConsumer()
    s_consumer.main()


if __name__ == '__main__':
    '''
    Simple program that illustrates how to use Apache Kafka. It assumes kafka is running on localhost:9092
    with one topic called test
    '''

    #start a squared consumer, fork it because service
    #Basically what this does is forks the SquaredConsumer.main(), that main method itself forks into four different processes
    #and waits to process incoming messages from Kafka. If none are encountered within 30 seconds the program kills itself.
    #Otherwise it outputs 100 squares
    worker = Consumer()
    worker.start()

    #start a squared producer no fork because it terminates.
    s_producer = SquaredProducer()
    s_producer.main()
    print 'created/published/ended producer'




