#!/bin/python3

from sys import argv
from kafka import KafkaProducer
from time import sleep
from json import dumps
# from socketio import connect, send_message
from simplesocket import socketing

producer = KafkaProducer(acks=1, compression_type='gzip', bootstrap_servers=['winubuntu:9092', 'namubuntu:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

if len(argv) > 1:
    topic = argv[1]
else:
    topic = 'tweet1'
if len(argv) > 2:
    cnt = argv[2]
else:
    cnt = 10
socketing()
# connect(1)
for i in range(cnt):
    data = {'str': 'result'+str(i)}
    print(data)
    # send_message(data)
    producer.send(topic, value=data)

    sleep(5)

# execute command
# python3 ~/pythonfiles/kafka/kafka/app/producer.py tweet1 100
