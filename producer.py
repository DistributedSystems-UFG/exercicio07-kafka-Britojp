from kafka import KafkaProducer
from const import *
import sys

topic = TOPIC1
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
for i in range(100):
    msg = 'My ' + str(i) + 'st message for topic ' + TOPIC1 + ' ' + str(i)
    print ('Sending message: ' + msg)
    producer.send(TOPIC1, value=msg.encode())
producer.flush()
