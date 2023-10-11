#!/bin/python3

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sockets import sio_app

app = FastAPI()
app.mount('/', app=sio_app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get('/')
async def home():
    return {'home': 'message'}


if __name__ == '__main__':
    kwargs = {"host": "0.0.0.0", "port": 8833}
    kwargs.update({"reload": True})
    uvicorn.run('main:app', **kwargs)
    # port=8833, host='0.0.0.0', reload=True)
