from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:19092",  # Connect to the first broker's advertised listener only
    "group.id": "meta-inspect",
    "auto.offset.reset": "earliest"
})

# Fetch metadata with a timeout to avoid hanging if the cluster is unreachable
consumer_instance = consumer.list_topics(timeout=10.0) 

# Brokers
print("Brokers in the cluster:")
for broker_id, broker in sorted(consumer_instance.brokers.items()):
    print(f"- Broker ID: {broker_id}, Host: {broker.host}, Port: {broker.port} {'(Controller)' if broker_id == consumer_instance.controller_id else ''}")

# Topics, partitions, leaders
print("\nTopics and their partitions:")
for tname, tmeta in sorted(consumer_instance.topics.items()):
    if tname.startswith("topic-"):
        for pid, pmeta in tmeta.partitions.items():
            print(f"- Topic: {tname}, Partition: {pid}, Leader: {pmeta.leader}")
