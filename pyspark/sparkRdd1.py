#!/bin/python3

from pyspark import SparkContext

sc = SparkContext("local", "first app")
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
# sc = spark.sparkContext

print("\n------------ Basic ------------------------------------------\n")
animals = ("dog", "cat", "frog", "horse")
rdd = sc.parallelize(animals)
print("getNumPartitions: ", rdd.getNumPartitions())
print("first: ", rdd.first())
print("take(1): ", rdd.take(1))
print("count: ", rdd.count())
print("filter", rdd.filter(lambda x: "o" in x).collect())

print("\n------------ Word Count ------------------------------------------\n")
rdd1 = sc.textFile("/data/wordSample.txt")
print("rdd: ", rdd1.collect())
rddcsv = rdd1.flatMap(lambda line: line.split(" "))
print("flatMap: ", rddcsv.collect())
rddgrp = rddcsv.map(lambda word: (word, 1))
print("groupby: ", rddgrp.collect())
rddcount = rddgrp.reduceByKey(lambda a, b: a + b)
print("reduceByKey: ", rddcount.collect())


spark.stop()
