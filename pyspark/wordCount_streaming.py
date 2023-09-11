#!/bin/python3

from pyspark.sql import SparkSession

if __name__=="__main__":
    
    spark=SparkSession.builder\
            .appName("WordStream")\
            .master("local[*]")\
            .getOrCreate()
    
    df=spark.readStream\
        .format("socket")\
        .option("host","winubuntu")\
        .option("port",9999)\
        .load()
    
    word_df=df.selectExpr("explode(split(value,' ')) as word")
    word_count=word_df.groupBy("word").count()
    
    write_stream=word_count.writeStream\
        .format("console")\
        .outputMode("complete")\
        .start()
    write_stream.awaitTermination()