#!/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
# import sys
# sys.setrecursionlimit(10**7)


def sparksql():
    spark = SparkSession \
        .builder \
        .appName("airline_python") \
        .master("local[1]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # schema = StructType([
    #     StructField("airlineId", IntegerType(), nullable=True),
    #     StructField("title", StringType(), nullable=True),
    #     StructField("genres", StringType(), nullable=True)
    # ])
    airline_data_path = "/data/1988_small.csv"
    airline_df = spark.read\
        .option("header", "true")\
        .csv(airline_data_path, inferSchema=True)
    print("\n------------dataframe schema-------------------------\n")
    airline_df.printSchema()

    print("\n------------column list-----------------------\n")
    print(airline_df.columns)
    print("column count: ", len(airline_df.columns))
    print("data count: ", airline_df.count())
    print("\n------------show()-----------------------------\n")
    airline_df.show(5)
    print("\n------------select show()-----------------------------\n")
    airline_df.select(
        "Year",
        "Month",
        "Origin",
        "Dest",
        "ActualElapsedTime",
        "AirTime",
    ).show(5)

    print("\n------------describe-----------------------------\n")
    airline_df.select(
        "ActualElapsedTime",
        "AirTime",
        "DepDelay",
        "ArrDelay"
    ).describe().show()

    print("\n------------summary-----------------------------\n")
    airline_df.select(
        "ActualElapsedTime",
        "AirTime",
        "DepDelay",
        "ArrDelay"
    ).summary().show()

    print("\n------------summary detail----------------------------\n")
    airline_df.select(
        "ActualElapsedTime",
        "AirTime",
        "DepDelay",
        "ArrDelay"
    ).summary(
        "count",
        "mean",
        "stddev",
        "min",
        "0%",
        "5%",
        "25%",
        "50%",
        "75%",
        "95%",
        "100%",
        "max"
    ).show()

    spark.stop()


if __name__ == "__main__":
    sparksql()
