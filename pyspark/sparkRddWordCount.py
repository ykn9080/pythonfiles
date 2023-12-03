#!/bin/python3

from pyspark import SparkContext

sc = SparkContext("local[1]", "wordcount")
sc.setLogLevel("ERROR")
rdd = sc.textFile("/data/wordSample.txt")
rddcsv = rdd.flatMap(lambda line: line.split(" "))
rddgrp = rddcsv.map(lambda word: (word, 1))
rddcount = rddgrp.reduceByKey(lambda a, b: a + b)


print("\n------------ Word Count ------------------------------------------\n\n")

print("file데이터를 읽어서 rdd생성: sc.textFile(\"/data/wordSample.txt\")")
rdd1 = sc.textFile("/data/wordSample.txt")
print(rdd1.collect(), "\n")

print("flatMap으로 multiline데이터 한번에 split: rdd1.flatMap(lambda line: line.split(" "))")
rddcsv = rdd1.flatMap(lambda line: line.split(" "))
print(rddcsv.collect(), "\n")

print("map으로 (word,1)형태의 array생성: rddcsv.map(lambda word: (word, 1))")
rddgrp = rddcsv.map(lambda word: (word, 1))
print(rddgrp.collect(), "\n")

print("reduceByKey로 word별 합계: rddgrp.reduceByKey(lambda a, b: a + b)")
rddcount = rddgrp.reduceByKey(lambda a, b: a + b)
print(rddcount.collect())


sc.stop()
