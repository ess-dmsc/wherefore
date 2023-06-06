import pytest
from wherefore.KafkaConsumer import KafkaConsumer
from stub_consumer import get_stub_consumer


@pytest.fixture
def empty_consumer():
    stub = get_stub_consumer({})
    return KafkaConsumer(stub)


@pytest.fixture
def consumer():
    topics_and_partitions = {
        "topic1": 1,
        "topic2": 3,
        "topic3": 1,
    }
    stub = get_stub_consumer(topics_and_partitions)
    return KafkaConsumer(stub)


def test_topics_listing_empty_consumer(empty_consumer):
    topics = empty_consumer.topics()
    assert len(topics) == 0


def test_topics_listing(consumer):
    topics = consumer.topics()
    assert "topic0" not in topics
    assert "topic1" in topics


def test_partitions_for_topic_single_partition(consumer):
    partitions = consumer.partitions_for_topic("topic1")
    assert 0 in partitions
    assert 1 not in partitions


def test_partitions_for_topic_multiple_partitions(consumer):
    partitions = consumer.partitions_for_topic("topic2")
    assert len(partitions) == 3
    assert 0 in partitions
    assert 1 in partitions
    assert 2 in partitions
    assert 3 not in partitions


def test_partitions_for_missing_topic(consumer):
    partitions = consumer.partitions_for_topic("missing")
    assert partitions is None
