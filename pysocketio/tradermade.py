#!/bin/python3

import socketio

# standard Python
sio = socketio.Client()


@sio.event
def connect():
    print("I'm connected!")
    sio.emit('login', {'userKey': 'HUcjO2LEP437NO20kl-r'})


@sio.event
def connect_error():
    print("The connection failed!")


@sio.event
def message(data):
    print('I received a message!')


@sio.on('handshake')
def on_message(data):
    print('HandShake', data)
    sio.emit('symbolSub', {'symbol': 'USDJPY'})
    sio.emit('symbolSub', {'symbol': 'GBPUSD'})
    sio.emit('symbolSub', {'symbol': 'EURUSD'})


@sio.on('price')
def on_message(data):
    print('Price Data ', data)


sio.connect('https://marketdata.tradermade.com')
