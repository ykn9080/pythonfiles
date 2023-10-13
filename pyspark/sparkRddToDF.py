#!/bin/python3


from pyspark.sql.types import StructType, StructField, StringType
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName(
    'SparkByExamples.com').getOrCreate()

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
rdd = spark.sparkContext.parallelize(dept)

df = rdd.toDF()
df.printSchema()
df.show(truncate=False)

deptColumns = ["dept_name", "dept_id"]
df2 = rdd.toDF(deptColumns)
df2.printSchema()
df2.show(truncate=False)

deptDF = spark.createDataFrame(rdd, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

deptSchema = StructType([
    StructField('dept_name', StringType(), True),
    StructField('dept_id', StringType(), True)
])

deptDF1 = spark.createDataFrame(rdd, schema=deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)

spark.stop()


"""
<h3>Test</h3>
<pre style='background-color: transparent; color: black;margin:0;padding:0'>are you ok
im no ok
</pre>
<div>are you ok<span>
<span>are you ok<span>
"""
