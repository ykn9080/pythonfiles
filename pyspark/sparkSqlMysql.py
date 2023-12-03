#!/usr/bin/python3

from pyspark.sql import SparkSession

# session 연결
spark = SparkSession.builder.appName("pysparkMysql")\
    .master("local[1]")\
    .config("spark.jars", "/usr/local/spark/jars/mysql-connector-java-8.0.19.jar")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.format("jdbc")\
    .option("url", "jdbc:mysql://localhost:3306/sakila")\
    .option("driver", "com.mysql.jdbc.Driver")\
    .option("dbtable", "customer")\
    .option("user", "root")\
    .option("password", "9080")\
    .load()

df.show()
spark.stop()
