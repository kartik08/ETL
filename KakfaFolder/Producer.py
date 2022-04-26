from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers="ec2-99-79-37-229.ca-central-1.compute.amazonaws.com:9092")
i = 0
def uploading(filename):
    i=0
    with open(f"D:\Docker\ETL\Dataset\{filename}.json") as fp:
        for line in fp:
            lineData = json.dumps(line)
            producer.send(filename,lineData.encode('utf-8'))