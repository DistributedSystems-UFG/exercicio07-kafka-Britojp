from kafka import KafkaConsumer
from const import *
import sys

# Create consumer: Option 1 -- only consume new events
consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

# Create consumer: Option 2 -- consume old events (uncomment to test -- and comment Option 1 above)
#consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT], auto_offset_reset='earliest')

consumer.subscribe([TOPIC1, TOPIC2])
for msg in consumer:
    print (msg.value)
