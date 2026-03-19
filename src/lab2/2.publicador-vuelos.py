import requests
import json 
from kafka import KafkaProducer
from time import sleep

producer = KafkaProducer(
  bootstrap_servers="d10v25ju3l09un9dm2d0.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
  security_protocol="SASL_SSL",
  sasl_mechanism="SCRAM-SHA-256",
  sasl_plain_username="juan",
  sasl_plain_password="123456",
  value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# AVIATION API parameters
access_key = "c11698d2c4e4dea3560905befee21e2d"
flight_status = "active"
arr_iata = "BCN"
aviation_url = f"http://api.aviationstack.com/v1/flights?access_key={access_key}&arr_iata=BCN&limit=1&flight_status=active"

#Topic
topic_name = "vuelos-topic"

# Main code
while True:
    # Obtener los vuelos desde la API de AviationStack
    response = requests.get(aviation_url)
    if response.status_code != 200:
        print(f"Algo ocurrió. ERROR: {response.status_code}")
        print(response.json())
        exit()

    # Procesamiento de la respuesta JSON
    json_text = response.text
    python_obj = json.loads(json_text)

    data_array = []

    for flight in python_obj['data']:
        data = {}
        print(flight)
        # Filtrar vuelos activos
        print()
        data['vuelo'] = flight['flight']['iata']
        data['aerolinea'] = flight['airline']['name']
        data['fecha_vuelo'] = flight['flight_date']
        data['origen'] = flight['departure']['airport']
        data['destino'] = flight['arrival']['airport']
        
        print(f"Enviando información del vuelo {data['vuelo']}")
        data_array.append(data)

    print("\nEnviando array: ")
    print(data_array)

    # Publicar en Kafka
    producer.send(topic_name, value=data_array)
    producer.flush()  # 
    sleep(10)