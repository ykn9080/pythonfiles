#!/bin/python3

from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder.appName("SocketStreaming").master("local[*]")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "winubuntu:9092,namubuntu:9092") \
        .option("subscribe", "tweet") \
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # df=spark.readStream\
    # .format("kafka")\
    # .option("kafka.bootstrap.servers","localhost:9092")\
    # .option("subscribe","tweet")\
    # .option("startingOffsets","earliest")\
    # .load()
    # .format("socket")\
    # .option("host","winubuntu")\
    # .option("port",9999)\
    # .load()

    print(df.isStreaming)
    print(df.printSchema())

    write_stream = df.writeStream\
        .format("console")\
        .outputMode("append")\
        .start()
    write_stream.awaitTermination()

    print("Application Completed")
