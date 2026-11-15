from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, size, explode, col

spark = SparkSession.builder \
    .appName("ModeloCoocurrencia_Amazon") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider") \
    .getOrCreate()

# Rutas actualizadas
ruta_limpia = "s3a://accesorios-appliances/datos-limpios/"
ruta_salida = "s3a://accesorios-appliances/recomendaciones-coocurrencia/"

print("Leyendo datos limpios...")
df = spark.read.parquet(ruta_limpia)

print("Calculando co-ocurrencias...")
interacciones = df.groupBy("user_id").agg(collect_set("parent_asin").alias("items_vistos"))

pares = interacciones.select(
    explode("items_vistos").alias("item_A")
).join(interacciones, size(col("items_vistos")) > 1) \
 .select("item_A", explode("items_vistos").alias("item_B")) \
 .filter(col("item_A") != col("item_B"))

recomendaciones_coocurrencia = pares.groupBy("item_A", "item_B").count() \
    .withColumnRenamed("count", "fuerza_coocurrencia") \
    .orderBy(col("fuerza_coocurrencia").desc())

recomendaciones_coocurrencia.write.mode("overwrite").parquet(ruta_salida)
print("¡Recomendaciones por Co-ocurrencia guardadas en S3!")