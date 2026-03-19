from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

admin = KafkaAdminClient(
  bootstrap_servers="d10v25ju3l09un9dm2d0.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
  security_protocol="SASL_SSL",
  sasl_mechanism="SCRAM-SHA-256",
  sasl_plain_username="juan",
  sasl_plain_password="123456",
)

try:
  topic = NewTopic(name="dmc2-topic", num_partitions=1, replication_factor=-1, replica_assignments=[])
  admin.create_topics(new_topics=[topic])
  print("Created topic")
except TopicAlreadyExistsError as e:
  print("Topic already exists")
finally:
  admin.close()