#!/bin/python3

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "sparkRdd").master("local[2]").getOrCreate()

moviePath = "/data/movieLens/movies.csv"
ratingPath = "/data/movieLens/ratings.csv"

movie = spark.read.csv(moviePath, header=True, inferSchema=True)
movie.printSchema()
movie.show(5, truncate=False)
spark.stop()
