import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, rand
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from dotenv import load_dotenv

spark = SparkSession.builder.appName("KafkaExample").getOrCreate()

def get_recommendation(song_name,data):
        song_cluster = data.filter(data.name == song_name).select('prediction').head()[0]
        cluster_data = data.filter(data.prediction == song_cluster)
        sampled_songs = cluster_data.select("name","artists","release_date").orderBy(rand()).limit(10).collect()
        return sampled_songs

load_dotenv()
data=spark.read.format('json').load('res')
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "172.17.0.2:9092").option("subscribe", "mazeniss").load()
value=df.selectExpr('CAST(value AS STRING)')
query: StreamingQuery = value.writeStream.format('console').outputMode('append').start()
value_df=df.selectExpr("CAST(value AS STRING)")
loaded_data=spark.read.format('json').load('res')
songs=get_recommendation(value_df,loaded_data)
mongo_schema = StructType([
            StructField("song_name", StringType(), True),
            StructField("recommended_songs", ArrayType(StructType([StructField("name", StringType(), True),StructField("artists", StringType(), True), StructField("release_date", StringType(),True)]) ), True)])
query_df=spark.createDataFrame([(value_df,songs)],mongo_schema)
query_df.show()
query_df.write.format("mongodb").option("spark.mongodb.connection.uri", os.getenv("DB_URI")).option("spark.mongodb.database", "test").option("spark.mongodb.collection", "firsts").mode("append").save()

query.awaitTermination()