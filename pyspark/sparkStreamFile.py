#!/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    spark = SparkSession \
        .builder\
        .appName("StreamFile")\
        .master("local[2]")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType()\
        .add("Date", TimestampType(), True)\
        .add("Message", StringType(), True)

    streamDF = spark\
        .readStream\
        .option("delimiter", "|")\
        .schema(schema)\
        .csv("file:///home/yknam/pythonfiles/pyspark/data/")

    streamDF.createOrReplaceTempView("stream")
    outDF = spark.sql("select * from stream")
    print(spark.sql('show tables from default').show())
    query = outDF\
        .writeStream\
        .outputMode("update")\
        .format("console")\
        .start()

    ssc = StreamingContext(spark.sparkContext, 1)
    ssc.awaitTerminationOrTimeout(10)
