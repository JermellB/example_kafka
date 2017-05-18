from kafka import KafkaProducer
from settings import KAFKA_CONNECTION
import time

class SquaredProducer(object):
    """
    Class that encapsulates a connection to kafka/topic and publishes 100 integers into the topic
    """

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=KAFKA_CONNECTION)
        time.sleep(3) #wait for full init

    def main(self):

        #create 100 ints and quit
        for _ in xrange(100):
            self.producer.send('test', bytes(_))

if __name__ == '__main__':
    producer = SquaredProducer()
    producer.main()