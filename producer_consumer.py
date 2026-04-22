from kafka import KafkaProducer
from const import *
import sys
from kafka import KafkaConsumer

producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
msg1 = 'Olá João'

msg2 = 'Olá Maria'

producer.send(TOPIC1, value=msg1.encode())
producer.send(TOPIC2, value=msg2.encode())

msg3 = 'Olá João e Maria'
producer.send(TOPIC1, value=msg3.encode())
producer.send(TOPIC2, value=msg3.encode())

producer.flush()

consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
consumer.subscribe([TOPIC1, TOPIC2])
for msg in consumer:
    print (msg.value.decode())