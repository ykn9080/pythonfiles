#!/bin/python3

import socketio

# Socket.IO 서버 생성
sio = socketio.Server()

# 연결 이벤트 핸들러


@sio.event
def connect(sid, environ):
    print('클라이언트가 연결되었습니다:', sid)

# 메시지 이벤트 핸들러


@sio.event
def message(sid, data):
    print('클라이언트로부터 메시지 수신:', data)


# 실행할 때 웹 소켓 서버 시작
if __name__ == '__main__':
    app = socketio.WSGIApp(sio)
    socketio.server.SocketIOServer(('localhost', 8000), app).serve_forever()
