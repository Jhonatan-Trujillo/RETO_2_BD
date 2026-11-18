from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, size, explode, col

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("ModeloCoocurrencia_Amazon") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Rutas en S3
ruta_limpia = "s3a://accesorios-appliances/datos-limpios/"
ruta_salida = "s3a://accesorios-appliances/recomendaciones-coocurrencia/"

print("Leyendo datos limpios desde S3...")
df = spark.read.parquet(ruta_limpia)

# Tomamos una muestra del 5% de los datos para que el join no colapse la memoria
print("Tomando una muestra del 5% de los datos para agilizar el proceso...")
df_muestra = df.sample(fraction=0.05, seed=42)

print("Calculando pares de productos que aparecen juntos...")
# Agrupamos por usuario los productos que ha comprado
interacciones = df_muestra.groupBy("user_id").agg(collect_set("parent_asin").alias("items_vistos"))

# Generamos los pares A-B y B-A
pares = interacciones.select(
    explode("items_vistos").alias("item_A")
).join(interacciones, size(col("items_vistos")) > 1) \
 .select("item_A", explode("items_vistos").alias("item_B")) \
 .filter(col("item_A") != col("item_B"))

# Contamos cuántas veces aparece cada par (fuerza de co-ocurrencia)
recomendaciones_coocurrencia = pares.groupBy("item_A", "item_B").count() \
    .withColumnRenamed("count", "fuerza_coocurrencia") \
    .orderBy(col("fuerza_coocurrencia").desc())

# 4. Guardar en S3
print("Guardando recomendaciones por co-ocurrencia en S3...")
recomendaciones_coocurrencia.write.mode("overwrite").parquet(ruta_salida)
print("¡Recomendaciones por Co-ocurrencia guardadas exitosamente en S3!")