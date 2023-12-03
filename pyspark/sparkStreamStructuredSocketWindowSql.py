#!/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder\
        .appName("StructuredStreamWordCountWindowSql")\
        .master("local[2]")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    lines = spark\
        .readStream\
        .format("socket")\
        .option("host", "localhost")\
        .option("port", 9999)\
        .load()
    lines.createOrReplaceTempView("word_table")
    wordCounts = spark.sql("""
                select window,
                      word,
                      COUNT(word)
                from 
                      (
                      select explode(split(value, " ")) word,
                        current_timestamp() ts
                      from word_table
                      )
                where word!=''
                GROUP BY window(ts,'10 seconds', '5 seconds'), word
                """)

    query = wordCounts\
        .writeStream\
        .outputMode("update")\
        .format("console")\
        .option("truncate", "false")\
        .start()

    query.awaitTermination()
