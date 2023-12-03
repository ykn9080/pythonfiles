#!/bin/python3

from pyspark import SparkContext

sc = SparkContext("local[*]", "first app")
sc.setLogLevel("ERROR")

print("\n------------ parallelize ------------------------------------------\n")
animals = [("dog", 10), ("cat", 5), ("frog", 100), ("horse", 1)]
rdd = sc.parallelize(animals)
print("parallelize로 rdd생성 rdd.collect(): ", rdd.collect())
print("rdd.getNumPartitions(): ", rdd.getNumPartitions())
print("rdd.first(): ", rdd.first())
print("rdd.take(1): ", rdd.take(1))
print("rdd.count(): ", rdd.count())
print("rdd.filter(lambda x:\"o\" in x).collect()",
      rdd.filter(lambda x: "o" in x[0]).collect())

print("\n------------ rdd로 새로운 rdd생성 -------------------------------\n")
rdd1 = rdd.map(lambda row: {(row[0], row[1]+100)})
print("rdd1.collect(): ", rdd1.collect())
