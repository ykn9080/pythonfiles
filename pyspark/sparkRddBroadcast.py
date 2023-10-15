#!/bin/python3

from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
# conf.set("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.0")
conf.set("spark.driver.memory", "2g")
conf.set("spark.executor.memory", "2g")

spark = SparkSession.builder \
    .appName("test") \
    .master("spark://winubuntu:7077") \
    .config(conf=conf) \
    .getOrCreate()


print("\n------------ Broadcast변수 생성 ------------------------------------------\n")
states = {"NY": "New York", "CA": "California", "FL": "Florida"}
broadcastStates = spark.sparkContext.broadcast(states)
print("broadcastStates: ", broadcastStates.value)

print("\n------------ DataFrame 생성 ------------------------------------------\n")
data = [("James", "Smith", "USA", "CA"),
        ("Michael", "Rose", "USA", "NY"),
        ("Robert", "Williams", "USA", "CA"),
        ("Maria", "Jones", "USA", "FL")
        ]
schema = ["firstname", "lastname", "country", "state"]
df = spark.createDataFrame(data=data, schema=schema)
df.show(truncate=False)

print("\n------------ Broadcast join ------------------------------------------\n")
joinDF = df.rdd.map(lambda x: (
    x[0], x[1], x[2], broadcastStates.value[x[3]])).toDF(schema)
joinDF.show(truncate=False)
