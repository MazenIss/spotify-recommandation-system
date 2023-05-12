import os
from pymongo import MongoClient
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dotenv import load_dotenv


# Define the MongoDB connection properties
mongo_url= os.getenv("DB_URI")
mongo_collection= 'first'

# Create a connection to MongoDB
mongo_client = MongoClient(mongo_url)


# Create a sample document in the collection
mongo_collection = mongo_client['spotify'][mongo_collection]
mongo_collection.insert_one({
    'id': 'sample_id',
    'name': 'sample_name',
    'popularity': 1,
    'duration_ms': 1000,
    'explicit': 1,
    'artists': 'sample_artist',
    'id_artists': 'sample_id_artist',
    'release_date': '2022-01-01',
    'danceability': 0.5,
    'energy': 0.5,
    'key': 1,
    'loudness': 0.5,
    'mode': 1,
    'speechiness': 0.5,
    'acousticness': 0.5,
    'instrumentalness': 0.5,
    'liveness': 0.5,
    'valence': 0.5,
    'tempo': 120,
    'time_signature': 4
})

# Infer the schema from the sample document
schema = StructType([
    StructField('id', StringType()),
    StructField('name', StringType()),
    StructField('popularity', IntegerType()),
    StructField('duration_ms', IntegerType()),
    StructField('explicit', IntegerType()),
    StructField('artists', StringType()),
    StructField('id_artists', StringType()),
    StructField('release_date', StringType()),
    StructField('danceability', DoubleType()),
    StructField('energy', DoubleType()),
    StructField('key', IntegerType()),
    StructField('loudness', DoubleType()),
    StructField('mode', IntegerType()),
    StructField('speechiness', DoubleType()),
    StructField('acousticness', DoubleType()),
    StructField('instrumentalness', DoubleType()),
    StructField('liveness', DoubleType()),
    StructField('valence', DoubleType()),
    StructField('tempo', DoubleType()),
    StructField('time_signature', IntegerType())
])
