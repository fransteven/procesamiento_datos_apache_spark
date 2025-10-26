# Women Clothing E-commerce — Streaming con Kafka + Spark

Repositorio con los scripts para simular ventas en tiempo real (producer), procesarlas con Spark Structured Streaming (consumer) y ejecutar un EDA batch.  
Dataset base: "Women Clothing E-commerce Sales Data" (Kaggle).

## Estructura del repositorio
- `producer.py`        — Producer Kafka: simula ventas y envía eventos JSON al topic `sales_data`.
- `consumer.py`        — Consumer PySpark: Spark Structured Streaming que lee desde Kafka y calcula agregados por ventana.
- `eda_batch.py`       — Script de análisis batch (EDA) local con pandas (opcional).
- `requirements.txt`   — Dependencias Python (kafka-python, pandas, etc.)
- `README.md`          — (este archivo)

> Ajusta rutas y nombres según tu VM y ubicación de Spark/Kafka en `/opt/Kafka` u otro directorio.

---

## Requisitos previos (en la VM Linux)
- Java JDK instalado (requerido por Kafka y Spark).
- Apache Kafka instalado (ej. en `/opt/Kafka`).
- Apache Spark instalado y `spark-submit` disponible.
- Python 3.8+ instalado.
- Paquetes Python: `kafka-python`, `pandas`. (Se indica instalación abajo.)

---

## Instalación de dependencias Python
Recomiendo usar un virtualenv:

```bash
# en el directorio del repo
python3 -m venv venv
source venv/bin/activate

# instalar dependencias
pip install --upgrade pip
pip install -r requirements.txt
