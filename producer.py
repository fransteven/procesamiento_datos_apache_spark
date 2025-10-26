"""
Kafka Producer - Women Clothing Sales
Simula ventas en tiempo real
"""

import time
import json
import random
from kafka import KafkaProducer

print("=" * 60)
print("KAFKA PRODUCER - Sales Data Stream")
print("Topic: sales_data")
print("=" * 60)

# Crear productor
producer = KafkaProducer(
    # Se especifica a qué broker debe conectarse el producer
    # para obtener los metadatos del clúster
    bootstrap_servers=["localhost:9092"],
    # Convierte el objeto de python a bytes
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

print("✅ Conectado a Kafka\n")

# Datos de ejemplo
colors = ["Black", "White", "Red", "Blue", "Green", "Pink", "Gray"]
sizes = ["XS", "S", "M", "L", "XL", "XXL"]
skus = ["SKU001", "SKU002", "SKU003", "SKU004", "SKU005"]


def generate_sale():
    """Genera una venta simulada"""
    return {
        "order_id": random.randint(10000, 99999),
        "order_date": time.strftime("%Y-%m-%d"),
        "sku": random.choice(skus),
        "color": random.choice(colors),
        "size": random.choice(sizes),
        "unit_price": random.randint(20, 150),
        "quantity": random.randint(1, 5),
        "revenue": 0,  # Se calculará en el consumer
        "timestamp": int(time.time()),
    }


print("🚀 Enviando datos... (Ctrl+C para detener)\n")

contador = 0

try:
    while True:
        # Generar venta
        sale = generate_sale()
        sale["revenue"] = sale["unit_price"] * sale["quantity"]

        # Enviar a Kafka
        producer.send("sales_data", value=sale)

        contador += 1
        print(
            f"[{contador}] Order #{sale['order_id']} | "
            f"{sale['sku']} | {sale['color']}-{sale['size']} | "
            f"${sale['revenue']}"
        )

        time.sleep(1)

except KeyboardInterrupt:
    print(f"\n⏹️ Detenido. Total enviado: {contador}")
finally:
    producer.close()
    print("✅ Conexión cerrada")
