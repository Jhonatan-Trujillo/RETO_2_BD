from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

MAX_ITEMS = 50   # tope de ítems por usuario (evita explosión de pares con usuarios muy activos)
TOP_N = 20       # vecinos que se guardan por ítem

spark = SparkSession.builder \
    .appName("ModeloCoocurrencia_Amazon") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

ruta_limpia = "s3a://accesorios-appliances/datos-limpios/"
ruta_salida = "s3a://accesorios-appliances/recomendaciones-coocurrencia/"

df = spark.read.parquet(ruta_limpia)

# 1. Un registro por (usuario, ítem), quedándonos con los MAX_ITEMS más recientes de cada usuario
w = Window.partitionBy("user_id").orderBy(F.col("timestamp").desc())
base = df.select("user_id", F.col("parent_asin").alias("item"), "timestamp") \
         .withColumn("n", F.row_number().over(w)) \
         .filter(F.col("n") <= MAX_ITEMS) \
         .select("user_id", "item").distinct()

# 2. Self-join POR USUARIO: pares de ítems que compró la misma persona
a, b = base.alias("a"), base.alias("b")
pares = a.join(b, F.col("a.user_id") == F.col("b.user_id")) \
         .filter(F.col("a.item") != F.col("b.item")) \
         .groupBy(F.col("a.item").alias("item_a"), F.col("b.item").alias("item_b")) \
         .agg(F.count("*").alias("fuerza_coocurrencia")) \
         .persist(StorageLevel.MEMORY_AND_DISK)

# 3. Top-N vecinos por ítem
w2 = Window.partitionBy("item_a").orderBy(F.col("fuerza_coocurrencia").desc(), F.col("item_b"))
top = pares.withColumn("rank", F.row_number().over(w2)).filter(F.col("rank") <= TOP_N)

# 4. Guardar en S3
top.write.mode("overwrite").parquet(ruta_salida)
print(f"Pares totales: {pares.count()} | guardados (top {TOP_N} por ítem): {top.count()}")