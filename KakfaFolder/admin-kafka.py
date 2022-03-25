from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer
adminClient = KafkaAdminClient( bootstrap_servers="localhost:9092")
def getListOfTopic():
    consumer =KafkaConsumer(group_id='test', bootstrap_servers="localhost:9092")
    return  list(consumer)

def createTopic(topicList):
    presentTopic = getListOfTopic()
    toCreateTopic = []
    for topic in topicList:
        if topic not in presentTopic:
            toCreateTopic.append(NewTopic(name="yelpDataset", num_partitions=1, replication_factor=1))
    adminClient.create_topics(new_topics=toCreateTopic, validate_only=False)