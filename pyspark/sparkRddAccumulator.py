#!/bin/python3

from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
# conf.set("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.0")
# conf.set("spark.driver.memory", "2g")
# conf.set("spark.executor.memory", "2g")

spark = SparkSession.builder \
    .appName("accumulatorTest") \
    .master("spark://winubuntu:7077") \
    .getOrCreate()


print("\n------------ Accumulator 변수 생성 ------------------------------------------\n")
file = spark.sparkContext.textFile("/data/wordSample.txt")
accum = spark.sparkContext.accumulator(0)

print("\n------------ Accumulator 합계 ------------------------------------------\n")
# arr = file.flatMap(lambda x:  x.split(" ") if x != "" else blankLines.add(1))

words = file.flatMap(lambda x: x.split(" "))
print("word flatMap list: ", words.collect())
words.foreach(lambda x: accum.add(1))
print("accumulator sum: ", accum.value)
