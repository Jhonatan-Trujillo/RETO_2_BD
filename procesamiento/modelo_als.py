from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import explode, col

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("ModeloALS_Amazon") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Rutas
ruta_limpia = "s3a://accesorios-appliances/datos-limpios/"
ruta_salida = "s3a://accesorios-appliances/recomendaciones-als/"

print("Leyendo datos limpios desde S3...")
df = spark.read.parquet(ruta_limpia)

# 3. Preparar datos para ALS
# Aseguramos que user_id y parent_asin sean tratados como índices numéricos
from pyspark.ml.feature import StringIndexer

indexer_user = StringIndexer(inputCol="user_id", outputCol="user_index", handleInvalid="skip")
indexer_item = StringIndexer(inputCol="parent_asin", outputCol="item_index", handleInvalid="skip")

df_indexed = indexer_user.fit(df).transform(df)
df_indexed = indexer_item.fit(df_indexed).transform(df_indexed)

df_ratings = df_indexed.select(
    col("user_index").cast("int"), 
    col("item_index").cast("int"), 
    col("rating").cast("float")
)

print("Entrenando modelo ALS (esto puede tomar unos minutos)...")
als = ALS(
    maxIter=10, 
    regParam=0.1, 
    userCol="user_index", 
    itemCol="item_index", 
    ratingCol="rating", 
    coldStartStrategy="drop"
)
modelo = als.fit(df_ratings)

print("Generando Top 10 recomendaciones por usuario...")
recomendaciones = modelo.recommendForAllUsers(10)

# 4. Aplanar y guardar
recomendaciones_planas = recomendaciones.select(
    "user_index", 
    explode("recommendations").alias("rec")
).select(
    "user_index", 
    col("rec.item_index").alias("item_recomendado"), 
    col("rec.rating").alias("score_predicho")
)

recomendaciones_planas.write.mode("overwrite").parquet(ruta_salida)
print("¡Recomendaciones ALS guardadas en S3!")