# from socketIO_client import SocketIO, LoggingNamespace

# socketIO = SocketIO('winubuntu', 8882, LoggingNamespace)


# def on_connect():
#     print('connect')


# def on_disconnect():
#     print('disconnect')


# def reciever(*args):
#     print('reciever', args)


# socketIO.on('connect', on_connect)
# socketIO.emit('message', "Hii I am Python Client")

# socketIO.on('message', reciever)
# socketIO.on('disconnect', on_disconnect)


# socketIO.wait()

import socketio

sio = socketio.Client()


@sio.event
def message(data):
    print('I received a message!')


@sio.event
def sendmessage(data):
    sio.emit('send_message', data)
    print('I sent a message'+JSON.stringify(data))


@sio.event
def joinRoom(roomId):
    sio.emit('join_room', roomId)
    print('send join room id:' + roomId)


@sio.on('send_message')
def on_message(data):
    sio.emit('send_message', data)
    print('I received a message!')


# sio.emit('send_message', {'image': 'encoded_image'})
sio.connect('http://winubuntu:8881', transports='websocket')
joinRoom(1)
sendmessage({"roomId": 1, "user": "young", "msg": 'hi', "time": '2019-01-01'})
