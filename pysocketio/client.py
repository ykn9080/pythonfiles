import asyncio
from time import sleep

import socketio
import requests

sio_client = socketio.AsyncClient()

r = requests.get("http://localhost:8833/")  # server prints "test"
print("print r:", r)


@sio_client.event
async def connect():
    print('I\'m connected')


@sio_client.event
async def disconnect():
    print('I\'m disconnected')


@sio_client.on("join")
async def join(data):
    print("joined:", data)


@sio_client.on("chat")
async def chat(data):
    print("chattingL:", data)


async def main():
    await sio_client.connect(url='http://localhost:8833', socketio_path='sockets')
    await sio_client.wait()
    for i in range(5):

        await sio_client.emit("chat", f"hello hello monkey {i}")
    # await sio_client.disconnect()

asyncio.run(main())
