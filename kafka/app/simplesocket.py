from socketIO_client import SocketIO, LoggingNamespace

socketIO = SocketIO('winubuntu', 8881, LoggingNamespace)


def on_connect():
    print('connect')


def on_disconnect():
    print('disconnect')


def reciever(*args):
    print('reciever', args)


socketIO.emit('join_room', 1)
socketIO.emit('send_msg', {
              roomId: 1, user: 'yknam', msg: 'hello111', time: '2020-01-01 12:00:00'})

socketIO.on('receive_msg', reciever)
socketIO.on('disconnect', on_disconnect)

socketIO.on('connect', on_connect)


socketIO.wait()
