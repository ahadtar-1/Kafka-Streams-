import logging
import json
import threading
from confluent_kafka import Consumer, KafkaException

consumer_config_event = {
    "bootstrap.servers": "localhost:9093",
    "group.id": "EVENTONE",
    "auto.offset.reset": "earliest",
}


def print_assignment(consumer, partitions):
    """
    Printing partitions
    """
    print("Assignment:", partitions)


class KafkaConsumer:
    """
    Class for handling kafka consumer
    """

    def __init__(self, creds, topic):

        if not isinstance(topic, list):
            topic = [topic]

        self.__topic = topic
        self.__logger = self.__get_logger()
        self.__consumer = Consumer(creds, logger=self.__logger)

        self.__consumer.subscribe(self.__topic, on_assign=print_assignment)

    def __get_logger(self):
        """
        Setting up logger
        """

        logger = logging.getLogger("consumer")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)-15s %(levelname)-8s %(message)s")
        )
        logger.addHandler(handler)

        return logger

    def close(self):
        """
        Closes kafka consumer
        """

        self.__consumer.close()

    def __decode_msg(self, msg):
        """
        Decodes a byte stream kafka message and return a dictionary.

        :params msg: (str) byte string

        :return: (Dict) dictionary representing the JSON msg
        """

        return msg.value().decode("utf-8")

    def start(self):
        """
        Initiate Kafka consumer
        """
        try:
            while True:
                #logging.error("it has stgarted")
                #print("It has started")
                msg = self.__consumer.poll(timeout=1.0)
                #print(msg)
                if msg is None:
                    continue
                    print("There is no message")
                if msg.error():
                    raise KafkaException(msg.error())
                    print("There is an error")

                # Proper message
                msg = self.__decode_msg(msg)

                logging.error(self.__topic[0] + "-->>" + str(msg))

        except Exception as _e:
            logging.error(_e)

        #finally:
            #self.__consumer.close()


if __name__ == "__main__":
    consumer_event = KafkaConsumer(consumer_config_event, "out_topic")
    consumer_event.start()
