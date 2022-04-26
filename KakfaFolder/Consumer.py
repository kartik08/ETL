from kafka import KafkaConsumer
from json import loads
from multiprocessing import Pool
import  pymongo


client = pymongo.MongoClient("mongodb+srv://shahk:Kartik%401998@cluster0.drb14.mongodb.net/test?authSource=admin&replicaSet=atlas-h601s6-shard-0&readPreference=primary&ssl=true")
db = client.FinalProject


topicList = ['consumer_yelp_academic_dataset_business', 'consumer_yelp_academic_dataset_checkin', 'consumer_yelp_academic_dataset_review', 'consumer_yelp_academic_dataset_tip', 'consumer_yelp_academic_dataset_user']

def mongoUpload(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['ec2-99-79-37-229.ca-central-1.compute.amazonaws.com:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='test',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    for event in consumer:
        print(event)
        # msg = event.value.decode('utf-8')
        data = loads(event.value)
        db[topic].insert_one(data)



if __name__ == "__main__":
    p = Pool()
    for topic in topicList:
        p.apply_async(mongoUpload(topic))
    p.close()
    p.join()


