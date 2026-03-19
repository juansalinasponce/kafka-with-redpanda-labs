import json
import mysql.connector
from kafka import KafkaConsumer
import socket

# Configuración de Kafka
consumer = KafkaConsumer(
    bootstrap_servers="d10v25ju3l09un9dm2d0.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username="juan",
    sasl_plain_password="123456",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=1000000,
    group_id="grupo-vuelos" 
)
consumer.subscribe(["vuelos-topic"])

# Configuración de la base de datos MySQL
db_config = {
    "host": "195.179.239.0",  # Cambiar por el host de tu base de datos
    "user": "u349685578_admin",       # Cambiar por el usuario de tu base de datos
    "password": "@Atokongo2025",  # Cambiar por la contraseña de tu base de datos
    "database": "u349685578_clases"  # Cambiar por el nombre de la base de datos
}

# Conexión a la base de datos
db_connection = mysql.connector.connect(**db_config)
cursor = db_connection.cursor()

# Función para insertar datos en la base de datos
def insert_into_db(data):
    print(data)
    user = 'Juan SP'
    insert_query = """
        INSERT INTO vuelos (
            vuelo, aerolinea, fecha_vuelo, origen, destino,user
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        data['vuelo'], data['aerolinea'], data['fecha_vuelo'], data['origen'], 
        data['destino'],user
    ))
    db_connection.commit()

# Consumir mensajes de Kafka y guardarlos en MySQL
for message in consumer:
    try:
        # Decodificar el mensaje JSON de Kafka
        message_value = json.loads(message.value.decode("utf-8"))
        for flight in message_value:
            insert_into_db(flight)
            print(f"Guardado vuelo {flight['vuelo']} en la base de datos.")
        consumer.commit()
    except Exception as e:
        print(f"Error procesando el mensaje: {e}")

# Cerrar conexión con la base de datos y Kafka
cursor.close()
db_connection.close()
consumer.close()