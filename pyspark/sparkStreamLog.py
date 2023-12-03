#!/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    spark = SparkSession \
        .builder\
        .appName("Nginxlog")\
        .master("local[2]")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType()\
        .add("value", StringType(), True)

    streamDF = spark\
        .readStream\
        .schema(schema)\
        .text("/data/sparktest")
        # .text("file:///var/log/nginx/access.log")

    streamDF.createOrReplaceTempView("nginxlog")
    outDF = spark.sql("select * from nginxlog")
    # print(outDF.show(truncate=False))
    # print(spark.sql('show tables from default').show())
    query = outDF\
        .writeStream\
        .outputMode("update")\
        .format("console")\
        .start()

    ssc = StreamingContext(spark.sparkContext, 1)
    ssc.awaitTerminationOrTimeout(100)
