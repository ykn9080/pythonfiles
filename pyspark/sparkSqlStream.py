#!/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import asyncio
import socketio

sio_client = socketio.AsyncClient()


@sio_client.event
async def connect():
    print('I\'m connected sparkSqlStream.py')


async def main():
    spark = SparkSession \
        .builder \
        .appName("movie_python") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    lines = spark\
        .readStream\
        .format("socket")\
        .option("host", "localhost")\
        .option("port", 9999)\
        .load()

    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )

    wordCounts = words.groupBy("word").count()

    query = wordCounts\
        .writeStream\
        .outputMode("complete")\
        .format("console")\
        .start()
    await sio_client.connect(url='http://localhost:8833', socketio_path='sockets')
    await sio_client.emit("chat", wordCounts)
    await sio_client.wait()
    query.awaitTermination()


if __name__ == '__main__':
    asyncio.run(main())
