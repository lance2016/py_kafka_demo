
from loguru import logger as log
import threading
import datetime
import json
import time
import uuid

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError


topic = 'example_topic'
# consumer = KafkaConsumer(topic, group_id='test_group2', bootstrap_servers='localhost:9092')


def test_producer():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    print('begin')
    try:
        n = 0
        while True:
            dic = {}
            dic['id'] = n
            n = n + 1
            dic['myuuid'] = str(uuid.uuid4().hex)
            dic['time'] = datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")
            producer.send(topic, json.dumps(dic).encode())
            log.info("send:" + json.dumps(dic))
            time.sleep(10)
    except KafkaError as e:
        print(e)
    finally:
        producer.close()
        print('done')


def test_consumer():
    consumer = KafkaConsumer('example_topic',
                             #  auto_offset_reset='earliest',
                             group_id='my-group',
                             bootstrap_servers='localhost:9092')
    # consumer.seek(TopicPartition(topic=u'example_topic',partition=None), 240)
    for msg in consumer:
        log.info(f"receive: {msg}")


if __name__ == '__main__':
    p = threading.Thread(target=test_producer)
    c = threading.Thread(target=test_consumer)
    p.start()
    c.start()
