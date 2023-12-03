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
        .master("local[*]") \
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

    movie_df.printSchema()

    movie_df.sort(col("title")).show(truncate=False)
    movie_df.explain()
    movie_df.filter(col("genres").contains("Romance")).select(
        "title").show(5, truncate=False)
    movie_df.groupBy("genres").count().show(5, False)
    print(movie_df)
    # ratings_path = "file:///home/yknam/movielens/ratings.csv"
    # ratings_df = spark.read\
    #     .option("header", "true")\
    #     .option("inferSchema", "true")\
    #     .csv(ratings_path)
    # print(ratings_df)
    # ratings_df.printSchema()
    # ratings_df.show(5, False)

    # join_df = movie_df.join(ratings_df, movie_df.movieId == ratings_df.movieId)
    # join_df.select('title', 'rating').orderBy(col("rating").desc()).show(5, False)

    # movie_df.createOrReplaceTempView("movie")
    # ratings_df.createOrReplaceTempView("rating")
    # spark.sql("""
    #     select title,rating from movie a join rating b
    #           on a.movieId=b.movieId
    # """).show(20, False)
    spark.stop()
    # return '{hello:"world"}'


def parse_csv(df):
    res = df.to_json(orient="records")
    parsed = json.loads(res)
    return parsed


if __name__ == "__main__":
    sparksql()
