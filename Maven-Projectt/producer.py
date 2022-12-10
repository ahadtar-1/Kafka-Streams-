from threading import Thread
import asyncio
import confluent_kafka
from confluent_kafka import KafkaException
import json

producer_config_event = {
    "bootstrap.servers": "localhost:9093",
}

TOPIC_NAME = 'data_topic'

dictionary = { 
  	"Id": "1", 
  	"Name": "ahad", 
  	"Competency": "Machine Learning",
  	"State": "Punjab",
  	"City": "Lahore"
	} 

class KafkaProducer:

    """
    Main class to handle Kafka Producer functionality
    """

    def __init__(self, configs):
        self._loop = asyncio.get_event_loop()
        self._producer = confluent_kafka.Producer(configs)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _poll_loop(self):
        """
        Poll loop for producer:

        :return: None
        """
        while not self._cancelled:
            self._producer.poll(0.1)

    def close(self):
        """
        To close the kafka connecttion

        :return: None
        """
        self._cancelled = True
        self._poll_thread.join()

    def produce(self, topic, key=None, value=None, partition=None):
        """
        An awaitable produce method, to produce events

        :params topic: (str) name of the topic
        :params value: (str) message payload
        :params key: (str) key for the payload

        :return: result of the trasaction
        """
        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(
                    result.set_exception, KafkaException(err)
                )
            else:
                self._loop.call_soon_threadsafe(result.set_result, msg)

        if key is None:
            self._producer.produce(topic, value=value, key=key, on_delivery=ack)
        else:
            self._producer.produce(
                topic, value=value, on_delivery=ack, partition=partition
            )

        return result
	
	
if __name__ == "__main__":
	producer = KafkaProducer(producer_config_event) 
	json_object = json.dumps(dictionary) 
	producer.produce(TOPIC_NAME,value=json_object)
	print("Working")     	
