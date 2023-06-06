from datetime import datetime
from os import getpid
from typing import Dict, List, Set
from uuid import uuid1
from confluent_kafka import Consumer, TopicPartition, KafkaException
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
            raise RuntimeError(e.args)

        return [Message(m) for m in msgs]

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

    def _get_offsets(self, topic, partition):
        tp = TopicPartition(topic, partition)

        try:
            offsets = self._consumer.get_watermark_offsets(tp)
        except KafkaException as e:
            raise RuntimeError(e.args)

        return offsets

    def topics(self) -> Set[str]:
        """
        Get topic names.

        :return: Set of topic names.
        """
        try:
            md = self._consumer.list_topics()
        except KafkaException as e:
            raise RuntimeError(e.args)

        topics = set()
        for k in md.topics.keys():
            topics.add(k)
        return topics

    def partitions_for_topic(self, topic: str) -> Set[int]:
        """
        Get partitions for topic.

        :param topic: Topic name.
        :return: Set of partitions for topic.
        """
        try:
            md = self._consumer.list_topics()
        except KafkaException as e:
            raise RuntimeError(e.args)

        try:
            tmd = md.topics[topic]
        except KeyError:
            return None

        partitions = set()
        for k in tmd.partitions.keys():
            partitions.add(k)

        return partitions

    def assign(self, topic: str, partitions: List[int]):
        """
        Assign topic and partitions to consumer.

        :param topic: Topic name.
        :param partitions: List of partitions.
        """
        tps = [TopicPartition(topic, p) for p in partitions]

        try:
            self._consumer.assign(tps)
        except KafkaException as e:
            raise RuntimeError(e.args)

    def offsets_for_times(self, partitions_and_times: Dict[int, int]) -> Dict[int, int]:
        """
        Get offsets per partition from timestamps in ms for assigned topic.

        :param partitions_and_times: Dict with partitions and timestamps in ms.
        :return: Dict with partitions and offsets (None if timestamp is after last message).
        """
        try:
            a = self._consumer.assignment()
        except KafkaException as e:
            raise RuntimeError(e.args)

        tps = []
        for tp in a:
            if tp.partition in partitions_and_times:
                tp.offset = partitions_and_times[tp.partition]
                tps.append(tp)

        try:
            oft = self._consumer.offsets_for_times(tps)
        except KafkaException as e:
            raise RuntimeError(e.args)

        r = {}
        for tp in oft:
            if tp.offset > 0:
                r[tp.partition] = tp.offset
            else:
                r[tp.partition] = None

        return r

    def seek(self, partition: int, offset: int):
        """
        Seek partition of assigned topic to given offset.

        :param partition: Partition.
        :param offset: Offset.
        """
        try:
            a = self._consumer.assignment()
        except KafkaException as e:
            raise RuntimeError(e.args)

        found = False
        for tp in a:
            if tp.partition == partition:
                found = True
                break

        if not found:
            raise RuntimeError(
                f"Partition {partition} not found in consumer assignment"
            )

        tp.offset = offset

        try:
            self._consumer.seek(tp)
        except KafkaException as e:
            raise RuntimeError(e.args)
