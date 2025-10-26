"""
EDA - Women Clothing E-Commerce Sales Data
Análisis Exploratorio de Datos (Batch Processing) - VERSIÓN SIMPLIFICADA
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as F_sum
import os

print("=" * 60)
print("ANÁLISIS EXPLORATORIO DE DATOS")
print("Dataset: Women Clothing E-Commerce Sales")
print("=" * 60)

# Crear sesión de Spark
spark = SparkSession.builder.appName("EDA_WomenClothing").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Crear directorios
os.makedirs("datos/processed", exist_ok=True)

# ============================================
# 1. CARGA DE DATOS
# ============================================
print("\n[1] CARGANDO DATOS...")

df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("datos/raw/women_clothing_sales.csv")
)

print(f"✅ Dataset cargado: {df.count()} filas x {len(df.columns)} columnas")

# ============================================
# 2. INSPECCIÓN BÁSICA
# ============================================
print("\n[2] ESTRUCTURA DEL DATASET")
df.printSchema()

print("\n[3] PRIMERAS 5 FILAS")
df.show(5)

# ============================================
# 3. ESTADÍSTICAS DESCRIPTIVAS
# ============================================
print("\n[4] ESTADÍSTICAS DESCRIPTIVAS")
df.describe().show()

# ============================================
# 4. ANÁLISIS DE NULOS
# ============================================
print("\n[5] VALORES NULOS")
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# ============================================
# 5. ANÁLISIS POR CATEGORÍAS
# ============================================
print("\n[6] TOP 10 PRODUCTOS MÁS VENDIDOS")
df.groupBy("sku").agg(F_sum("quantity").alias("total_vendido")).orderBy(
    col("total_vendido").desc()
).show(10)

print("\n[7] VENTAS POR COLOR")
df.groupBy("color").agg(
    count("*").alias("num_ordenes"), F_sum("revenue").alias("ingresos_totales")
).orderBy(col("ingresos_totales").desc()).show()

print("\n[8] VENTAS POR TALLA")
df.groupBy("size").agg(
    F_sum("quantity").alias("unidades_vendidas"), F_sum("revenue").alias("ingresos")
).orderBy(col("ingresos").desc()).show()

# ============================================
# 6. LIMPIEZA DE DATOS
# ============================================
print("\n[9] LIMPIANDO DATOS...")

filas_inicial = df.count()
df_clean = df.dropna()  # Elimina filas que contengan algún null en cualquier col
df_clean = df_clean.dropDuplicates()  # Elimina filas duplicadas exáctas
filas_final = df_clean.count()

print(f"   Antes: {filas_inicial} filas")
print(f"   Después: {filas_final} filas")
print(f"   Eliminadas: {filas_inicial - filas_final} filas")

# ============================================
# 7. GUARDAR RESULTADOS
# ============================================
print("\n[10] GUARDANDO RESULTADOS...")

df_clean.write.mode("overwrite").parquet("datos/processed/datos_limpios.parquet")

print("✅ Guardado: datos/processed/datos_limpios.parquet")

# ============================================
# RESUMEN FINAL
# ============================================
print("\n" + "=" * 60)
print("RESUMEN DEL EDA")
print("=" * 60)
print(
    f"""
📊 Dataset: {filas_final:,} órdenes
💰 Ingresos totales: ${df_clean.agg(F_sum('revenue')).first()[0]:,.2f}
📦 Unidades vendidas: {df_clean.agg(F_sum('quantity')).first()[0]:,}
"""
)

spark.stop()
print("\n✅ ANÁLISIS COMPLETADO")
