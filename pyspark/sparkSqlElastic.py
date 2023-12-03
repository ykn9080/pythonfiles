#!/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def elastic():
    spark = SparkSession \
        .builder \
        .appName("movie_python") \
        .master("local[1]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.format("org.elasticsearch.spark.sql")\
        .option("es.read.field.as.array.include", "NerArray")\
        .option("es.nodes", "namubuntu:9200")\
        .option("es.nodes.discovery", "true")\
        .load("index명")

    df.registerTempTable("ner")
    spark.sql("show tables").show()
    spark.sql("select * from ner").show()

    spark.stop()


if __name__ == "__main__":
    elastic()
