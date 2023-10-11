from kafka import KafkaProducer
import time
import random
import asyncio
import socketio
import requests


sio_client = socketio.AsyncClient()


@sio_client.event
async def connect():
    print('I\'m connected')


# @sio_client.event
# async def disconnect():
#     print('I\'m disconnected')


async def main():
    await sio_client.connect(url='http://localhost:8833', socketio_path='sockets')
    for i in range(5000):
        await sio_client.emit("chat", i)
    time.sleep(1)
    await sio_client.wait()


if __name__ == "__main__":
    asyncio.run(main())
