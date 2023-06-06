from typing import Union, Dict, List, Optional, Set
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient


def get_topic_partitions(
    broker: str,
    security_config: Dict[str, str],
) -> Optional[List[Dict[str, Union[str, Set[int]]]]]:
    """
    Get list of topics and associated partitions for the Kafka broker.

    :param broker: Broker address.
    :param security_config: Dict with security configuration.
    :return: List of topics and partitions.
    """  # TODO: update documentation
    try:
        security_config["bootstrap.servers"] = broker  # TODO
        c = AdminClient(security_config)
        metadata = c.list_topics()
        topics = []
        for tm in metadata.topics.values():
            partitions = set(p.id for p in tm.partitions.values())
            topics.append({"name": tm.topic, "partitions": partitions})
        return topics
    except KafkaException:
        return None
