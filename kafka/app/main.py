from flask import Flask, Response, jsonify, stream_with_context

from kafka import KafkaConsumer
from consumerJson import make_summary
app = Flask(__name__)


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route('/consume')
def consume():
    """Returning pizza orders"""
    consumer = KafkaConsumer(
        # topic="tweet",  # 읽어올 토픽의 이름 필요.
        # 어떤 서버에서 읽어 올지 지정
        bootstrap_servers=[
            'namubuntu:9092', 'winubuntu:9092'],
        # 어디서부터 값을 읽어올지 (earlest 가장 처음 latest는 가장 최근)
        auto_offset_reset="earliest",
        # enable_auto_commit=True,  # 완료되었을 떄 문자 전송
        # group_id='my-group', # 그룹핑하여 토픽 지정할 수 있다 > 같은 컨슈머로 작업
        # value_deserializer=lambda x: loads(x.decode('utf-8')), # 역직렬화 ( 받을 떄 ) ; 메모리에서 읽어오므로 loads라는 함수를 이용한다. // 직렬화 (보낼 떄)
        # 1000초 이후에 메시지가 오지 않으면 없는 것으로 취급.
        # consumer_timeout_ms=1000
    )
    consumer.subscribe(topics=["tweet"])
    cnt = 3
    array = []

    def consume_msg():
        for message in consumer:
            # if (cnt == 0):
            #     break
            # cnt = cnt-1
            yield (message.value.decode("utf-8"))

    return Response((consume_msg()))


@app.route('/consume2')
def consume2():
    return Response(make_summary())


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8883)
