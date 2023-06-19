from unittest.mock import MagicMock


class StubPartitionMetadata:
    def __init__(self, id_):
        self.id = id_


class StubTopicMetadata:
    def __init__(self, topic, partitions):
        self.topic = topic
        self.partitions = partitions


class StubClusterMetadata:
    def __init__(self, topics_and_partitions):
        # TODO document
        self.topics = {}
        for topic, partitions in topics_and_partitions.items():
            pms = {}
            for i in range(partitions):
                pms[i] = StubPartitionMetadata(i)
            self.topics[topic] = StubTopicMetadata(topic, pms)


def get_stub_consumer(topics_and_partitions):
    stub = MagicMock()
    stub.list_topics.return_value = StubClusterMetadata(topics_and_partitions)
    return stub
