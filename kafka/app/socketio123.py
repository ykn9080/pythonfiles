
import socketio

# standard Python
sio = socketio.Client()
sio.connect('http://winubuntu:8833')


@sio.event
def connect():
    print("I'm connected!")


@sio.event
def connect_error():
    print("The connection failed!")


# @sio.event
# def message(data):
#     print('I received a message!')


@sio.event
def send_message(data):
    sio.emit('join_room', 1)
    sio.emit(event='send_msg', data={
             "roomId": 1, "user": 'yknam', "msg": 'hello', "time": '2020-01-01 12:00:00'})
    print('data sent to server')


@sio.on('handshake')
def on_message(data):
    print('HandShake', data)
    sio.emit('send_msg', {'symbol': 'USDJPY'})
# data:{roomId:1, user:'yknam',msg:'hello',time:'2020-01-01 12:00:00'}


@sio.on('send_to_client')
def on_message(data):
    print('data from server ', data)


# send_message('hello')
