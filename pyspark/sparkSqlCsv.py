#!/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
# import sys
# sys.setrecursionlimit(10**7)


def sparksql():
    spark = SparkSession \
        .builder \
        .appName("movie_python") \
        .master("local[1]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    schema = StructType([
        StructField("movieId", IntegerType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("genres", StringType(), nullable=True)
    ])
    movie_data_path = "/data/movieLens/movies.csv"
    movie_df = spark.read\
        .option("header", "true")\
        .csv(movie_data_path, schema)
    print("\n------------dataframe schema-------------------------\n")
    movie_df.printSchema()

    print("\n------------sort by title-----------------------------\n")
    movie_df.sort(col("title")).show(truncate=False)

    print("\n------------explain dataframe-----------------------------\n")
    movie_df.explain()
    print("\n------------filter,select-----------------------------\n")
    movie_df.filter(col("genres").contains("Romance")).select(
        "title").show(5, truncate=False)
    print("\n------------group by-----------------------------\n")
    movie_df.groupBy("genres").count().show(5, False)

    print("\n------------rating dataframe -------------------------\n")
    ratings_path = "file:///home/yknam/movielens/ratings.csv"
    ratings_df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(ratings_path)
    ratings_df.printSchema()
    ratings_df.show(5, False)

    print("\n------------join movie and rating by movieId-------------------\n")
    join_df = movie_df.join(ratings_df, movie_df.movieId == ratings_df.movieId)
    join_df.select('title', 'rating').orderBy(
        col("rating").desc()).show(5, False)

    print("\n------------create tempview and sql-------------------\n")
    movie_df.createOrReplaceTempView("movie")
    ratings_df.createOrReplaceTempView("rating")
    spark.sql("""
        select title,rating from movie a join rating b
              on a.movieId=b.movieId
    """).show(5, False)
    print("\n------------table list-------------------\n")
    (spark.sql('show tables from default').show())
    spark.stop()


if __name__ == "__main__":
    sparksql()
