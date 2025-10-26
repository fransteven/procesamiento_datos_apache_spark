"""
Spark Streaming Consumer - Women Clothing Sales
Analiza ventas en tiempo real desde Kafka
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    LongType,
    TimestampType,
)
from pyspark.sql.functions import (
    from_json,
    col,
    from_unixtime,
    window,
    count,
    sum,
    avg,
)

print("=" * 60)
print("SPARK STREAMING CONSUMER")
print("Analizando ventas en tiempo real...")
print("=" * 60)

# Crear sesión de Spark
spark = SparkSession.builder.appName("SalesStreamingAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Definir esquema de los datos
schema = StructType(
    [
        StructField("order_id", IntegerType()),
        StructField("order_date", StringType()),
        StructField("sku", StringType()),
        StructField("color", StringType()),
        StructField("size", StringType()),
        StructField("unit_price", IntegerType()),
        StructField("quantity", IntegerType()),
        StructField("revenue", IntegerType()),
        StructField("timestamp", LongType()),
    ]
)

# Leer stream desde Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sales_data")
    .load()
)

# Parsear datos JSON
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convertir timestamp a TimestampType
parsed_df = parsed_df.withColumn(
    "event_time", from_unixtime(col("timestamp")).cast(TimestampType())
)

print("\n✅ Conectado a Kafka - Procesando datos...\n")

# ============================================
# ANÁLISIS 1: ESTADÍSTICAS GENERALES
# ============================================
print("[ANÁLISIS 1] Estadísticas por ventana de 1 minuto\n")

# Se crean ventanas deslizantes de 1 minuto basadas en event_time
# Se agrupan los eventos dentro de cada ventana
# Se calculan agregaciones por ventana
stats = parsed_df.groupBy(window(col("event_time"), "1 minute")).agg(
    count("*").alias("total_ordenes"),
    sum("revenue").alias("ingresos_totales"),
    sum("quantity").alias("unidades_vendidas"),
    avg("unit_price").alias("precio_promedio"),
)

# Escritura del resultado
query1 = (
    stats.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

# ============================================
# ANÁLISIS 2: VENTAS POR COLOR
# ============================================
print("[ANÁLISIS 2] Ventas por color en ventanas de 1 minuto\n")

ventas_color = (
    parsed_df.groupBy(window(col("event_time"), "1 minute"), "color")
    .agg(count("*").alias("num_ordenes"), sum("revenue").alias("ingresos"))
    .orderBy(col("ingresos").desc())
)

# Escritura del resultado
query2 = (
    ventas_color.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

# ============================================
# ANÁLISIS 3: TOP PRODUCTOS
# ============================================
print("[ANÁLISIS 3] Top productos por ingresos\n")

top_products = (
    parsed_df.groupBy(window(col("event_time"), "1 minute"), "sku")
    .agg(sum("revenue").alias("ingresos_totales"), sum("quantity").alias("unidades"))
    .orderBy(col("ingresos_totales").desc())
)

query3 = (
    top_products.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

# ============================================
# ANÁLISIS 4: VENTAS POR TALLA
# ============================================
print("[ANÁLISIS 4] Distribución de ventas por talla\n")

ventas_size = parsed_df.groupBy(window(col("event_time"), "1 minute"), "size").agg(
    count("*").alias("ordenes"), sum("quantity").alias("unidades")
)

query4 = (
    ventas_size.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

print("\n" + "=" * 60)
print("✅ STREAMING ACTIVO - Procesando datos en tiempo real")
print("   Presiona Ctrl+C para detener")
print("=" * 60 + "\n")

# Esperar a que terminen todos los queries
query1.awaitTermination()
