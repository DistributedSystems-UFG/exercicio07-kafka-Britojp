from kafka import KafkaProducer
from const import *
import sys
from kafka import KafkaConsumer
import os
import random

def sumNumbers(numbers):
    return sum(numbers)

consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
consumer.subscribe([TOPIC1])
numbers = [random.randint(1, 100) for _ in range(10)]
txt = "The sum of the numbers is " + str(sumNumbers(numbers))
producer.send(TOPIC2, value=txt.encode())
producer.flush()