from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
topic = 's'

for i in range(100):
	producer.send(topic,  b'hihi', str(i))

