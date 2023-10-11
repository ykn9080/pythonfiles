import socketio

sio_server = socketio.AsyncServer(
    async_mode='asgi',
    cors_allowed_origins=[]
)


sio_app = socketio.ASGIApp(
    socketio_server=sio_server,
    socketio_path='sockets'
)


@sio_server.event
async def connect(sid, environ, auth):
    print(f'{sid}: connected')
    await sio_server.emit('join', {'sid': sid})


@sio_server.event
async def chat(sid, message):
    print("client:", message)
    await sio_server.emit('chat', {'sid': sid, 'message': message})


@sio_server.on("broadcast")
async def broadcast(sid, msg):
    print(f"broadcast {msg}")
    await sio_server.emit("event_name", msg)  # or send to everyone


@sio_server.event
async def disconnect(sid):
    print(f'{sid}: disconnected')
    await sio_server.emit('left', {'sid': sid})
