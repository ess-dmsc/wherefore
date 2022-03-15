from kafka import KafkaConsumer
from typing import Union, Dict, List, Optional
from kafka.errors import NoBrokersAvailable


def get_topic_partitions(
    broker: str,
) -> Optional[List[Dict[str, Union[str, List[int]]]]]:
    try:
        consumer = KafkaConsumer(bootstrap_servers=broker)
        known_topics = consumer.topics()
        list_of_topics = []
        for topic in known_topics:
            found_partitions = consumer.partitions_for_topic(topic)
            topic_entry = {"name": topic, "partitions": found_partitions}
            list_of_topics.append(topic_entry)
        return list_of_topics
    except NoBrokersAvailable:
        return None
