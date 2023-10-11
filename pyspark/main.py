from typing import Union
import uvicorn
from fastapi import FastAPI
import pandasTest
import word_generator
import sparkSql
import basic

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

@app.get("/pandas/")
def read_user():
    return pandasTest.read_data()

@app.get("/generator/")
def generate_word():
    return generate_word()
@app.get("/spark/sql")
def sparksql():
    return sparksql()
@app.get("/basic")
def basic():
    return basic()

if __name__ == '__main__':
    uvicorn.run(app, port=8832, host='0.0.0.0')