#!/bin/python3

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, collect_list
from pyspark.sql.functions import collect_set, sum, avg, max, countDistinct, count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
from pyspark.sql.functions import variance, var_samp,  var_pop

spark = SparkSession.builder.master("local[1]").appName(
    'SparkByExamples.com').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
simpleData = [("James", "Sales", 3000),
              ("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("Maria", "Finance", 3000),
              ("James", "Sales", 3000),
              ("Scott", "Finance", 3300),
              ("Jen", "Finance", 3900),
              ("Jeff", "Marketing", 3000),
              ("Kumar", "Marketing", 2000),
              ("Saif", "Sales", 4100)
              ]
schema = ["employee_name", "department", "salary"]

df = spark.createDataFrame(data=simpleData, schema=schema)
# df.printSchema()
# df.show(truncate=False)

# print("avg: " + str(df.select(avg("salary")).collect()[0][0]))
# print("avg: " + str(df.select(avg("salary")).collect()[0]))
# print("avg: " + str(df.select(avg("salary")).collect()))
# print(df.select(avg("salary")).collect())
df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)
print("Distinct Count of Department & Salary: "+str(df2.collect()[0][0]))
print("Distinct Count of Department & Salary: "+str(df2.collect()[0]))