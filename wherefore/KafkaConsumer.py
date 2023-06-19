"""Wrap Kafka library consumer.

The return types are mostly based on the previous types returned directly by
kafka-python (now replaced with confluent-kafka).
"""

from os import getpid
from typing import Dict, List, Set
from uuid import uuid1
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from wherefore.Message import Message


FETCH_MAX_BYTES = 52428800 * 6


def make_kafka_consumer(bootstrap_servers, security_config):
    group_id = f"wherefore-{getpid()}-{str(uuid1())}"
    config = {
        "bootstrap.servers": bootstrap_servers,
        "fetch.max.bytes": FETCH_MAX_BYTES,
        "group.id": group_id,
    }
    config.update(security_config)
    consumer = Consumer(config)
    return KafkaConsumer(consumer)


class KafkaConsumerError(Exception):
    pass


class NoBrokersAvailableError(KafkaConsumerError):
    pass


class KafkaConsumer:
    def __init__(self, consumer: Consumer, timeout_ms: int = 100):
        self._consumer = consumer
        self._timeout_s = timeout_ms / 1000

    def consume(self, max_msgs: int = 1) -> List[Message]:
        """
        Consume up to max messages or until timeout.

        :param topic: Topic name.
        :param partition: Partition.
        :return: List of messages.
        """
        try:
            msgs = self._consumer.consume(max_msgs, timeout=self._timeout_s)
        except KafkaException as e:
            self._raise_from_kafka_exception(e)

        result = []
        for m in msgs:
            result.append(Message(m.value(), m.timestamp()[1] / 1e3, m.offset()))

        return result

    def beginning_offset(self, topic: str, partition: int) -> int:
        """
        Get low offset for topic partition.

        :param topic: Topic name.
        :param partition: Partition.
        :return: Low offset.
        """
        offsets = self._get_offsets(topic, partition)
        return offsets[0]

    def end_offset(self, topic: str, partition: int) -> int:
        """
        Get high offset for topic partition.

        :param topic: Topic name.
        :param partition: Partition.
        :return: High offset.
        """
        offsets = self._get_offsets(topic, partition)
        return offsets[1]

    def _get_offsets(self, topic: str, partition: int):
        topic_partition = TopicPartition(topic, partition)

        try:
            offsets = self._consumer.get_watermark_offsets(topic_partition)
        except KafkaException as e:
            self._raise_from_kafka_exception(e)

        return offsets

    def topics(self) -> Set[str]:
        """
        Get topic names.

        :return: Set of topic names.
        """
        try:
            metadata = self._consumer.list_topics()
        except KafkaException as e:
            self._raise_from_kafka_exception(e)

        topics = set()
        for k in metadata.topics.keys():
            topics.add(k)

        return topics

    def partitions_for_topic(self, topic: str) -> Set[int]:
        """
        Get partitions for topic.

        :param topic: Topic name.
        :return: Set of partitions for topic.
        """
        try:
            metadata = self._consumer.list_topics()
        except KafkaException as e:
            self._raise_from_kafka_exception(e)

        try:
            topic_metadata = metadata.topics[topic]
        except KeyError:
            return None

        partitions = set()
        for k in topic_metadata.partitions.keys():
            partitions.add(k)

        return partitions

    def assign(self, topic: str, partitions: List[int]):
        """
        Assign topic and partitions to consumer.

        :param topic: Topic name.
        :param partitions: List of partitions.
        """
        topic_partitions = [TopicPartition(topic, p) for p in partitions]

        try:
            self._consumer.assign(topic_partitions)
        except KafkaException as e:
            self._raise_from_kafka_exception(e)

    def offsets_for_times(self, partitions_and_times: Dict[int, int]) -> Dict[int, int]:
        """
        Get offsets per partition from timestamps in ms for assigned topic.

        :param partitions_and_times: Dict with partitions and timestamps in ms.
        :return: Dict with partitions and offsets (None if timestamp is after last message).
        """
        try:
            assigned_partitions = self._consumer.assignment()
        except KafkaException as e:
            self._raise_from_kafka_exception(e)

        topic_partitions = []
        for tp in assigned_partitions:
            if tp.partition in partitions_and_times:
                tp.offset = partitions_and_times[tp.partition]
                topic_partitions.append(tp)

        try:
            topic_partition_offsets = self._consumer.offsets_for_times(topic_partitions)
        except KafkaException as e:
            self._raise_from_kafka_exception(e)

        result = {}
        for tp in topic_partition_offsets:
            if tp.offset > 0:
                result[tp.partition] = tp.offset
            else:
                result[tp.partition] = None

        return result

    def seek(self, partition: int, offset: int):
        """
        Seek partition of assigned topic to given offset.

        :param partition: Partition.
        :param offset: Offset.
        """
        try:
            assigned_partitions = self._consumer.assignment()
        except KafkaException as e:
            self._raise_from_kafka_exception(e)

        found = False
        for topic_partition in assigned_partitions:
            if topic_partition.partition == partition:
                found = True
                break

        if not found:
            raise RuntimeError(
                f"Partition {partition} not found in consumer assignment"
            )

        topic_partition.offset = offset

        try:
            self._consumer.seek(topic_partition)
        except KafkaException as e:
            self._raise_from_kafka_exception(e)

    def _raise_from_kafka_exception(self, exception: KafkaException):
        """Extract error code from KafkaException and raise appropriate exception."""
        kafka_error = exception.args[0]
        if kafka_error.code() == KafkaError.BROKER_NOT_AVAILABLE:
            raise NoBrokersAvailableError(exception.args)
        else:
            raise RuntimeError(exception.args)
