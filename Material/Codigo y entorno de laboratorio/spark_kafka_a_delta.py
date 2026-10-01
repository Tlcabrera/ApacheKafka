#!/usr/bin/env python3
"""Laboratorio 14 - Kafka a Delta con Spark Structured Streaming.

Ejecutar dentro del contenedor ``spark`` creado por docker-compose.etapaF.yml:
  spark-submit /opt/spark/work-dir/codigo/spark_kafka_a_delta.py

Las coordenadas de los conectores y la configuración S3A viven en
entorno/spark-defaults.conf. Así se evita que cada estudiante copie una lista
distinta de --packages. Los tres jobs usan checkpoints independientes.
"""
from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException
from pyspark.sql.functions import col, from_json, to_date, current_timestamp, expr
from pyspark.sql.types import StructType, StringType, DoubleType

BOOT = "broker1:19092"
TOPIC = "dian.facturacion.emitida"
BRONCE, PLATA = "s3a://dian-bronce/facturacion", "s3a://dian-plata/facturacion"
RECHAZOS = "s3a://dian-plata/facturacion_rechazos"
CKPT_B, CKPT_P = "s3a://dian-checkpoints/bronce", "s3a://dian-checkpoints/plata"

spark = (SparkSession.builder.appName("dian-facturacion-bronce-plata")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .getOrCreate())

esquema = (StructType()
           .add("id_evento", StringType()).add("nit_emisor", StringType())
           .add("cufe", StringType()).add("valor_total", DoubleType())
           .add("fecha_emision", StringType()).add("estado", StringType()))

# --- BRONCE: crudo tal como llegó, con marca de ingesta ---------------------
crudo = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", BOOT)
         .option("subscribe", TOPIC)
         .option("startingOffsets", "earliest")     # solo la primera vez; después manda el checkpoint
         .load())

bronce = (crudo
          .select(col("key").cast("string").alias("clave"),
                  from_json(col("value").cast("string"), esquema).alias("d"),
                  col("partition").alias("particion"), col("offset"),
                  col("timestamp").alias("ts_kafka"))
          .select("clave", "d.*", "particion", "offset", "ts_kafka")
          .withColumn("ingerido_en", current_timestamp())
          .withColumn("fecha", to_date(col("fecha_emision"))))

# Delta no permite abrir una fuente streaming de una ruta inexistente. Crear la
# tabla vacía una vez permite arrancar plata inmediatamente; después bronce la
# alimenta en paralelo. ``ignore`` preserva los datos en los reinicios.
try:
    spark.read.format("delta").load(BRONCE).limit(0).collect()
except AnalysisException:
    (spark.createDataFrame([], bronce.schema).write.format("delta")
     .partitionBy("fecha").mode("ignore").save(BRONCE))

q_bronce = (bronce.writeStream.format("delta")
            .option("checkpointLocation", CKPT_B)
            .partitionBy("fecha").outputMode("append").start(BRONCE))

# --- PLATA: deduplicado por id_evento, tipado y con tabla de rechazos -------
# La deduplicación es lo que protege plata: si se pierde únicamente el
# checkpoint de bronce, este vuelve a escribir archivos desde Kafka; plata ve
# esos commits nuevos, pero su propio estado de ``dropDuplicates`` conserva los
# id_evento ya procesados.
b = spark.readStream.format("delta").load(BRONCE)
validos = b.filter(col("id_evento").isNotNull() & (col("valor_total") > 0))
rechazos = b.filter(col("id_evento").isNull() | (col("valor_total") <= 0)) \
            .withColumn("motivo_rechazo", expr("CASE WHEN id_evento IS NULL THEN 'sin id' ELSE 'valor no positivo' END"))

plata = (validos.withWatermark("ts_kafka", "1 hour")
         .dropDuplicates(["id_evento"])
         .withColumn("valor_total", col("valor_total").cast("decimal(16,2)")))

q_plata = (plata.writeStream.format("delta").option("checkpointLocation", CKPT_P)
           .partitionBy("fecha").outputMode("append").start(PLATA))
q_rech = (rechazos.writeStream.format("delta")
          .option("checkpointLocation", CKPT_P + "_rechazos")
          .outputMode("append").start(RECHAZOS))

spark.streams.awaitAnyTermination()
