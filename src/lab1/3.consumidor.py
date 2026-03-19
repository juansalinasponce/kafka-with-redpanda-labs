from kafka import KafkaConsumer

consumer = KafkaConsumer(
  bootstrap_servers="d10v25ju3l09un9dm2d0.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
  security_protocol="SASL_SSL",
  sasl_mechanism="SCRAM-SHA-256",
  sasl_plain_username="juan",
  sasl_plain_password="123456",
  auto_offset_reset="earliest",#lastest
  enable_auto_commit=False,
  consumer_timeout_ms=100000
)
consumer.subscribe(["dmc-topic"])

for message in consumer:
  topic_info = f"topic: {message.topic} ({message.partition}|{message.offset})"
  message_info = f"key: {message.key}, {message.value}"
  print(f"{topic_info}, {message_info}")