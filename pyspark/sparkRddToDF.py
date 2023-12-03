#!/bin/python3


from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains

spark = SparkSession.builder.master("local[1]").appName(
    'SparkByExamples.com').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
dept = [("Finance", ["Java", "Scala", "C++"], 10), ("Marketing", ["Spark", "Java", "C++"], 20), ("Sales", ["CSharp", "VB"], 30), ("IT", ["Python", "VB"], 40)
        ]
rdd = spark.sparkContext.parallelize(dept)
# toDF()
df = rdd.toDF()
df.printSchema()
df.show(truncate=False)

# toDF(schema)
deptColumns = ["dept_name", "language", "dept_id"]
df2 = rdd.toDF(deptColumns)
df2.printSchema()
df2.show(truncate=False)

# createDataFrame(rdd,schema)
deptDF = spark.createDataFrame(rdd, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# createDataFrame(rdd,StractType)
deptSchema = StructType([
    StructField('dept_name', StringType(), True),
    StructField('languages', ArrayType(StringType()), True),
    StructField('dept_id', StringType(), True)
])

deptDF1 = spark.createDataFrame(rdd, schema=deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)

print("\n-----------------filter---------------------\n")
#  Condition
deptDF1.filter(deptDF1.dept_name == "Finance").show(truncate=False)

# SQL Expression
deptDF1.filter("dept_name == 'Finance'").show(truncate=False)

# Multiple condition
deptDF1.filter((deptDF1.dept_name == "Finance") & (
    deptDF1.dept_id == 10)).show(truncate=False)

# Array condition
deptDF1.filter(array_contains(deptDF1.languages, "Java")).show(truncate=False)


spark.stop()
