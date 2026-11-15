from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import explode, col

spark = SparkSession.builder \
    .appName("ModeloALS_Amazon") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider") \
    .getOrCreate()

# Rutas actualizadas
ruta_limpia = "s3a://accesorios-appliances/datos-limpios/"
ruta_salida = "s3a://accesorios-appliances/recomendaciones-als/"

print("Leyendo datos limpios...")
df = spark.read.parquet(ruta_limpia)

# Preparar datos
df_ratings = df.select(
    col("user_id"), 
    col("parent_asin").alias("item_id"), 
    col("rating").cast("float")
)

print("Entrenando modelo ALS...")
als = ALS(
    maxIter=10, 
    regParam=0.1, 
    userCol="user_id", 
    itemCol="item_id", 
    ratingCol="rating", 
    coldStartStrategy="drop"
)
modelo = als.fit(df_ratings)

print("Generando recomendaciones...")
recomendaciones = modelo.recommendForAllUsers(10)

recomendaciones_planas = recomendaciones.select(
    "user_id", 
    explode("recommendations").alias("rec")
).select(
    "user_id", 
    col("rec.item_id").alias("item_recomendado"), 
    col("rec.rating").alias("score_predicho")
)

recomendaciones_planas.write.mode("overwrite").parquet(ruta_salida)
print("¡Recomendaciones ALS guardadas en S3!")