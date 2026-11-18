from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Configurar Spark con el conector de Neo4j
spark = SparkSession.builder \
    .appName("CargarNeo4j_Amazon") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.neo4j:neo4j-connector-apache-spark_2.12:5.1.0_for_spark_3") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Rutas
ruta_limpia = "s3a://accesorios-appliances/datos-limpios/"

print("Leyendo datos limpios desde S3...")
df = spark.read.parquet(ruta_limpia)

# 3. Tomar una muestra manejable (importante para Neo4j local)
# Tomamos 5,000 registros para que el contenedor no colapse
df_muestra = df.select("user_id", "parent_asin", "rating").limit(5000)

# 4. Configurar la conexión a Neo4j (Usando el nombre del servicio 'neo4j')
neo4j_url = "bolt://neo4j:7687"
neo4j_user = "neo4j"
neo4j_password = "password123"

print("Cargando nodos Usuario...")
df_muestra.select(col("user_id").alias("user_id")).distinct().write \
    .format("org.neo4j.spark.DataSource") \
    .option("url", neo4j_url) \
    .option("authentication.basic.username", neo4j_user) \
    .option("authentication.basic.password", neo4j_password) \
    .option("labels", ":Usuario") \
    .option("node.keys", "user_id") \
    .mode("Overwrite") \
    .save()

print("Cargando nodos Item...")
df_muestra.select(col("parent_asin").alias("item_id")).distinct().write \
    .format("org.neo4j.spark.DataSource") \
    .option("url", neo4j_url) \
    .option("authentication.basic.username", neo4j_user) \
    .option("authentication.basic.password", neo4j_password) \
    .option("labels", ":Item") \
    .option("node.keys", "item_id") \
    .mode("Overwrite") \
    .save()

print("Cargando relaciones VIO...")
df_muestra.write \
    .format("org.neo4j.spark.DataSource") \
    .option("url", neo4j_url) \
    .option("authentication.basic.username", neo4j_user) \
    .option("authentication.basic.password", neo4j_password) \
    .option("relationship", "VIO") \
    .option("relationship.save.strategy", "keys") \
    .option("relationship.source.labels", ":Usuario") \
    .option("relationship.source.save.mode", "Match") \
    .option("relationship.source.node.keys", "user_id:user_id") \
    .option("relationship.target.labels", ":Item") \
    .option("relationship.target.save.mode", "Match") \
    .option("relationship.target.node.keys", "parent_asin:item_id") \
    .option("relationship.properties", "rating") \
    .mode("Overwrite") \
    .save()

print("¡Datos cargados exitosamente en Neo4j!")