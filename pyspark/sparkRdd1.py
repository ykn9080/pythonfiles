#!/bin/python3

from pyspark import SparkContext

sc = SparkContext("local", "first app")

rdd = sc.parallelize([1, 2, 3, 4, 5])
print("getNumPartitions: ", rdd.getNumPartitions())
print("first: ", rdd.first())
print("take(1): ", rdd.take(1))
print("count: ", rdd.count())
print("------------ Word Count ------------------------------------------")
rdd1 = sc.textFile("/data/wordSample.txt")
print("rdd: ", rdd1.collect())
rddcsv = rdd1.flatMap(lambda line: line.split(" "))
print("flatMap: ", rddcsv.collect())
rddgrp = rddcsv.map(lambda word: (word, 1))
print("groupby: ", rddgrp.collect())
rddcount = rddgrp.reduceByKey(lambda a, b: a + b)
print("reduceByKey: ", rddcount.collect())
