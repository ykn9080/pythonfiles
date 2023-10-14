#!/bin/python3

from pyspark import SparkContext

sc = SparkContext("local", "first app")


print("\n------------ Basic ------------------------------------------\n")
animals = ("dog", "cat", "frog", "horse")
rdd = sc.parallelize(animals)
print("parallelize로 rdd생성: ", rdd.collect())
print("first: ", rdd.first())
print("take(1): ", rdd.take(1))
print("count: ", rdd.count())
print("filter", rdd.filter(lambda x: "o" in x).collect())


print("\n------------ Word Count ------------------------------------------\n")
rdd1 = sc.textFile("/data/wordSample.txt")
print("file데이터를 읽어서 rdd생성: ", rdd1.collect())
rddcsv = rdd1.flatMap(lambda line: line.split(" "))
print("flatMap으로 multiline데이터 한번에 split: ", rddcsv.collect())
rddgrp = rddcsv.map(lambda word: (word, 1))
print("map으로 (word,1)형태의 array생성: ", rddgrp.collect())
rddcount = rddgrp.reduceByKey(lambda a, b: a + b)
print("reduceByKey로 word별 합계: ", rddcount.collect())


sc.stop()
