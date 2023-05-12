from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import Normalizer, StandardScaler
import random

import time

kafka_topic_name = "songTopic"
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession.builder.appName("Spotify Streaming Reccomendation System").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from songTopic
songs_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

songs_df1 = songs_df.selectExpr("CAST(value AS STRING)", "timestamp")


songs_schema_string = "order_id INT,id STRING, name STRING,popularity INT, duration_ms DOUBLE, explicit INT, " \
                           + "artists STRING, id_artists STRING, release_date STRING, " \
                           + "danceability DOUBLE," \
                           + "energy DOUBLE, key INT, loudness DOUBLE, " \
                           + "mode INT," \
                           + "speechiness DOUBLE," \
                           + "acousticness DOUBLE, instrumentalness DOUBLE, liveness DOUBLE, " \
                           + "valence DOUBLE, tempo DOUBLE, time_signature DOUBLE"



songs_df2 = songs_df1 \
        .select(from_csv(col("value"), songs_schema_string) \
                .alias("song"), "timestamp")


songs_df3 = songs_df2.select("song.*", "timestamp")

def simple():    
    songs_df3.createOrReplaceTempView("song_find");
    song_find_text = spark.sql("SELECT name, artists FROM song_find")
    songs_agg_write_stream = song_find_text \
            .writeStream \
            .trigger(processingTime='5 seconds') \
            .outputMode("update") \
            .option("truncate", "false") \
            .format("console") \
            .start()

    songs_agg_write_stream.awaitTermination()

    print("Songs Streaming...")
    
def csv_output():    
    song_find_text = spark.sql("SELECT name, artists FROM song_find")
    songs_agg_write_stream = song_find_text \
            .writeStream \
            .trigger(processingTime='5 seconds') \
            .outputMode("append") \
            .option("truncate", "false") \
            .option("path",'answ') \
            .option("checkpointLocation", "checkpoint_path") \
            .format("csv") \
            .start()

    songs_agg_write_stream.awaitTermination()

    print("Songs Streaming...")
    
songs_df3.createOrReplaceTempView("song_find");
song_find_text = spark.sql("SELECT * FROM song_find")
songs_agg_write_stream = song_find_text \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("append") \
        .option("truncate", "false") \
        .format("memory") \
        .queryName("testedTable5") \
        .start()

songs_agg_write_stream.awaitTermination(1)