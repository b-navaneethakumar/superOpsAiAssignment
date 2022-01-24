from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext, SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, split
import time

if __name__ == '__main__':

    spark = SparkSession.builder.appName("superOpsAI").getOrCreate()
    # read the tweet data from socket
    tweet_df = spark \
        .readStream \
        .format("socket") \
        .option("host", "0.0.0.0") \
        .option("port", 5599) \
        .load()

    # type cast the column value
    tweet_df_string = tweet_df.selectExpr("CAST(value AS STRING)")

    tweet_df_string.printSchema()
    # split words based on space, filter out hashtag values and group them up
    tweets_tab = tweet_df_string.withColumn('word', explode(split(col('value'), ' '))) \
        .withColumn('timestamp', current_timestamp().cast(TimestampType())) \
        .drop("value")

    windowDuration = "10 seconds"
    slideDuration = "5 seconds"

    windowedCounts = tweets_tab.withWatermark("timestamp", "5 minutes")\
.groupBy(window("timestamp", windowDuration, slideDuration),"word").count()

    windowedCountsDF = windowedCounts.drop("window").toDF("word","count")


    """
        since structured stream doesnt allow to do Order / sort by , Writing the streaming data into Memory and using spark sql query to get the functionality working
        
    """
    resultDF = windowedCountsDF.orderBy(desc("count")).writeStream.format("memory").queryName("MyInMemoryTable").outputMode("complete").start()

    outDF = spark.sql("select * from  ( select word,count , \
             row_number() over (partition by word order by count desc) as seqnum \
      from MyInMemoryTable ) a where seqnum <= 100 order by a.count desc")


    milliseconds = int(time.time() * 1000)
    outDF.write.parquet( f"./parc/{milliseconds}.parquet")

    # query = windowedCounts.orderBy(desc("count")) \
    #     .writeStream\
    #     .outputMode("append").format("parquet")\
    #     .option("path", "./parc")\
    #     .option("checkpointLocation", "./check")\
    #     .trigger(processingTime='60 seconds').start()
    #
    #windowedCounts.awaitTermination()

    print("----- streaming is running -------")