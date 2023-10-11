from kafka import KafkaProducer
import time
import random

KAFKA_TOPIC_NAME_CONS = "tweet"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'winubuntu:9092,namubuntu:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: x.encode('utf-8'))

    filepath = "/home/yknam/pythonfiles/kafka/app/words.txt"
    with open(filepath) as f:
        df = f.readlines()
        g = ''
        for f in df:
            fsub = f.replace("\n", "")
            g = g + fsub
            dfarr = g.split(' ')
    for lp in range(10):
        message = ''
        start = (random.randint(0, len(dfarr)-10))
        for i in range(start, start+10):
            message = message+" "+dfarr[i]
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(3)

        print(lp+1, ": ", message)

    print("Kafka Producer Application Completed. ")
