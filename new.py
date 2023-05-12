import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
import json

KAFKA_TOPIC_NAME_CONS = "issmazen"
KAFKA_BOOTSTRAP_SERVERS_CONS = '172.31.16.1:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    filepath = "tracks.csv"

    songs_df = pd.read_csv(filepath)
    songs_df = songs_df[songs_df['popularity'] > 50]

    songs_df['artists'] = songs_df['artists'].str.replace('[^a-zA-Z]', '')
    songs_df['id_artists'] = songs_df['id_artists'].str.replace('[^a-zA-Z]', '')

    song_list = songs_df.to_dict(orient="records")

    for message in song_list[:5]:
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)

    print("Kafka Producer Application Completed. ")
