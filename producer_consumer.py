from kafka import KafkaProducer
from const import *
import sys
from kafka import KafkaConsumer



consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT], auto_offset_reset='earliest')
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
consumer.subscribe([TOPIC1])
for msg in consumer:
    print (msg.value.decode())
    txt = "Message received: " + msg.value.decode() + " from topic " + TOPIC1
    producer.send(TOPIC2, value=txt.encode())
    producer.flush()