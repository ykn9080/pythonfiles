#!/bin/python3

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":

    sc = SparkContext("local[2]", "WindowsWordCount")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("./tmp")

    lines = ssc.socketTextStream("localhost", 9999)

    words = lines.flatMap(lambda line: line.split(" "))
    pairs = words.map(lambda word: (word, 1))
    wordWindows = pairs.reduceByKeyAndWindow(lambda x, y: x+y, 10, 5)
    wordWindows.pprint()

    ssc.start()
    ssc.awaitTermination()
