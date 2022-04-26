from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer
adminClient = KafkaAdminClient( bootstrap_servers="ec2-99-79-37-229.ca-central-1.compute.amazonaws.com:9092")

def getListOfTopic():
    consumer =KafkaConsumer(group_id='test', bootstrap_servers="ec2-99-79-37-229.ca-central-1.compute.amazonaws.com:9092")
    return  list(consumer.topics())

def createTopic(topicList):
    presentTopic = getListOfTopic()
    toCreateTopic = []
    for topic in topicList:
        if topic not in presentTopic:
            toCreateTopic.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
    adminClient.create_topics(new_topics=toCreateTopic, validate_only=False)