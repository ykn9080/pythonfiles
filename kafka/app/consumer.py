#!/bin/python3

from sys import argv
from kafka import KafkaConsumer
from json import loads  # 읽어오는 함수
import time
import datetime

if len(argv) > 1:
    topic = argv[1]
else:
    topic = 'tweet1'

consumer = KafkaConsumer(topic,  # 읽어올 토픽의 이름 필요.
                         # 어떤 서버에서 읽어 올지 지정
                         bootstrap_servers=[
                             'namubuntu:9092', 'winubuntu:9092'],
                         # 어디서부터 값을 읽어올지 (earlest 가장 처음 latest는 가장 최근)
                         auto_offset_reset="earliest",
                         enable_auto_commit=True,  # 완료되었을 떄 문자 전송
                         # group_id='my-group', # 그룹핑하여 토픽 지정할 수 있다 > 같은 컨슈머로 작업
                         # value_deserializer=lambda x: loads(x.decode('utf-8')), # 역직렬화 ( 받을 떄 ) ; 메모리에서 읽어오므로 loads라는 함수를 이용한다. // 직렬화 (보낼 떄)
                         # 1000초 이후에 메시지가 오지 않으면 없는 것으로 취급.
                         consumer_timeout_ms=1000
                         )
start = time.time()  # 현재 시간
print("START= ", start)
for message in consumer:
    topic = message.topic
    partition = message.partition
    offset = message.offset
    value = message.value
    timestamp = message.timestamp
    datetimeobj = datetime.datetime.fromtimestamp(timestamp/10000)

    print("Topic:{}, partition:{}, offset:{}, value:{}, datetimeobj:{}".format(
        topic, partition, offset, value, datetimeobj))

print("Elapsed time= ", (time.time()-start))  # 걸리는 시간


# execute command
# python3 ~/pythonfiles/kafka/kafka/app/consumer.py tweet1
