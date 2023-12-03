#!/usr/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql import Row

# session 연결
spark = SparkSession.builder.appName("pysparkMysql")\
    .master("local[1]")\
    .getOrCreate()

# .config("spark.jars", "/usr/local/spark/jars/mysql-connector-java-8.0.19.jar")\
spark.sparkContext.setLogLevel("ERROR")

studentDf = spark.createDataFrame([
    Row(name='vijay1', email="vijay@naver.com", password="67"),
    Row(name='Ajay1', email="ajay@naver.com", password="88"),
    Row(name='jay1', email="jay@naver.com", password="79"),
    Row(name='vinay1', email="vinay@naver.com", password="67"),
])

# Create DataFrame
columns = ["name", "email", "password"]
data = [("James", "james@yahoo.com", "MMM"), ("Ann", "ann@yahoo.com", "FFFF"),
        ("Jeff", "jeff@yahoo.com", "MMM"), ("Jennifer", "jen@yahoo.com", "FFF")]
sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

studentDf.select("name", "email", "password")\
    .write.mode("append").format("jdbc")\
    .option("url", "jdbc:mysql://localhost:3306/imcdb") \
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("dbtable", "users") \
    .option("user", "root").option("password", "9080")\
    .save()

table_1_query = "(select * from users order by id desc) a"
df = spark.read.format("jdbc")\
    .option("url", "jdbc:mysql://localhost:3306/imcdb")\
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("dbtable", table_1_query)\
    .option("user", "root")\
    .option("password", "9080")\
    .load()

df.show(10)

spark.stop()
