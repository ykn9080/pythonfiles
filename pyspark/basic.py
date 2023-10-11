#!/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql.types import Row
import json

def basic():
    spark=SparkSession.builder.appName("Basic").master("local[*]").getOrCreate()
    
    datalist = [Row(id=1, name="ykn", city='seoul'), Row(
        id=2, name="ykn2", city='seoul2'), Row(id=3, name="ykn3 is good", city='seoul2')]
    df = spark.createDataFrame(datalist)
    # word_df = df.selectExpr("explode(split(name,' ')) as word")
    # word_df = df.selectExpr("split(name,' ') as word")
    
    # word_df.show()
    message='{"message":"hello world"}'
    print(json.dumps(message).encode("utf-8"))
    df.show()
    spark.stop()

    return json.dumps(message).encode("utf-8")

    
if __name__ == "__main__":

    basic()