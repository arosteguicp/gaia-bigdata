from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, abs as spark_abs, current_timestamp, lit
from pyspark.sql.types import StructType, DoubleType

# Definir esquema del JSON
schema = StructType() \
    .add("source_id", DoubleType()) \
    .add("ra", DoubleType()) \
    .add("dec", DoubleType()) \
    .add("parallax", DoubleType()) \
    .add("pmra", DoubleType()) \
    .add("pmdec", DoubleType()) \
    .add("phot_g_mean_mag", DoubleType()) \
    .add("bp_rp", DoubleType()) \
    .add("teff_gspphot", DoubleType()) \
    .add("distance_gspphot", DoubleType())

# Crear sesión de Spark
spark = SparkSession.builder.appName("GaiaCleanerStreaming").getOrCreate()

# Leer de Kafka (streaming)
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "gaia_topic") \
    .load()

# Parsear JSON
df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Aplicar limpieza

df = df.withColumn("ra", when(col("ra") < 0, 0).when(col("ra") > 360, 360).otherwise(col("ra")))
df = df.withColumn("dec", when(col("dec") < -90, -90).when(col("dec") > 90, 90).otherwise(col("dec")))
df = df.withColumn("parallax", spark_abs(col("parallax")))
df = df.withColumn("pmra", when(col("pmra") < -200, -200).when(col("pmra") > 200, 200).otherwise(col("pmra")))
df = df.withColumn("pmdec", when(col("pmdec") < -200, -200).when(col("pmdec") > 200, 200).otherwise(col("pmdec")))
df = df.withColumn("phot_g_mean_mag", when(col("phot_g_mean_mag") < 3, 3).when(col("phot_g_mean_mag") > 21, 21).otherwise(col("phot_g_mean_mag")))
df = df.withColumn("bp_rp", when(col("bp_rp") < 0, spark_abs(col("bp_rp"))).when(col("bp_rp") > 5, 5).otherwise(col("bp_rp")))
df = df.withColumn("teff_gspphot", when(col("teff_gspphot") < 0, spark_abs(col("teff_gspphot"))).when(col("teff_gspphot") > 15000, 15000).otherwise(col("teff_gspphot")))
df = df.withColumn("distance_gspphot", when(col("distance_gspphot") < 0, spark_abs(col("distance_gspphot"))).when(col("distance_gspphot") > 50000, 50000).otherwise(col("distance_gspphot")))

# Agregar timestamp de procesamiento
df = df.withColumn("processed_at", current_timestamp())

# Guardar como Parquet
df.writeStream \
    .format("parquet") \
    .option("path", "/app/output/cleaned_stream/parquet") \
    .option("checkpointLocation", "/app/output/cleaned_stream/chk_parquet") \
    .outputMode("append") \
    .start()

# Guardar como CSV (opcional)
df.writeStream \
    .format("csv") \
    .option("path", "/app/output/cleaned_stream/csv") \
    .option("checkpointLocation", "/app/output/cleaned_stream/chk_csv") \
    .option("header", True) \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()