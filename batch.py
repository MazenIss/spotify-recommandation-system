from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
from pyspark.ml.feature import StandardScaler
from pyspark.sql.functions import count
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
# Create the SparkSession
spark = SparkSession.builder.appName('batch').getOrCreate()

# Load the CSV file into a DataFrame
#df = pd.read_csv('tracks.csv')
df = spark.read.format('csv').option('header', True).load('tracks.csv')



#feature engineering
df = df.orderBy(desc('release_date'))
df_stream = df
#df = df.drop('id','explicit','mode', 'release_date','id_artists', 'time_signature','duration_ms')
for col_name in ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']:
        df = df.withColumn(col_name, col(col_name).cast('float'))
assembler=VectorAssembler(inputCols=['danceability','energy','loudness','speechiness','acousticness','instrumentalness','liveness','valence','tempo'], outputCol='features')
assembled_data=assembler.setHandleInvalid("skip").transform(df)
assembled_data.show(5)
#scale features column
scale=StandardScaler(inputCol='features',outputCol='standardized')

data_scale=scale.fit(assembled_data)
df=data_scale.transform(assembled_data)
#save the scaler model
scaler_path = './scaler'
data_scale.save(scaler_path)


silhouette_score=[]
evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized',metricName='silhouette', distanceMeasure='squaredEuclidean')
KMeans_algo=KMeans(featuresCol='standardized', k=100)
KMeans_fit=KMeans_algo.fit(df)
output_df =KMeans_fit.transform(df)
silhouette_score = evaluator.evaluate(output_df)
print(silhouette_score)
cluster_sizes = output_df.groupBy('prediction').agg(count('*').alias('cluster_size'))
cluster_sizes.show()
output_df.select('name','prediction').show(10)

KMeans_fit.save("kmeans_model")
output_df.write.format('json').mode('overwrite').save('res')
