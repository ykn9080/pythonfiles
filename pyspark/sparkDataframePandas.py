#!/bin/python3

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.pandas as ps


def sparkDataframeToPandas():
    spark = SparkSession \
        .builder \
        .appName("movie_pandas") \
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

    df = ps.DataFrame(movie_df)
    df2 = df.groupby(['genres']).sum()
    print(df2)

# # Create pandas DataFrame
# technologies = ({
#     'Courses': ["Spark", "PySpark", "Hadoop", "Python", "Pandas", "Hadoop", "Spark", "Python", "NA"],
#     'Fee': [22000, 25000, 23000, 24000, 26000, 25000, 25000, 22000, 1500],
#     'Duration': ['30days', '50days', '55days', '40days', '60days', '35days', '30days', '50days', '40days'],
#     'Discount': [1000, 2300, 1000, 1200, 2500, None, 1400, 1600, 0]
# })
# df = pd.DataFrame(technologies)
# print(df)


# # Use groupby() to compute the sum
# df2 = df.groupby(['Courses']).sum()
# print(df2)


if __name__ == "__main__":
    sparkDataframeToPandas()
