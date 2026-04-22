from kafka import KafkaProducer
from const import *
import sys
from kafka import KafkaConsumer

producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
msg1 = 'Olá João'
print ('Sending message: ' + msg1)

msg2 = 'Olá Maria'
print ('Sending message: ' + msg2)

producer.send(TOPIC1, value=msg1.encode())
producer.send(TOPIC2, value=msg2.encode())

msg3 = 'Olá João e Maria'
print ('Sending message: ' + msg3)
producer.send(TOPIC1, value=msg3.encode())
producer.send(TOPIC2, value=msg3.encode())

producer.flush()

consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
consumer.subscribe([TOPIC1])
for msg in consumer:
    print (msg.value)