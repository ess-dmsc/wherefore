from threading import Thread
from typing import Union, Dict
from datetime import datetime
from enum import Enum, auto
from kafka import KafkaConsumer, TopicPartition
from queue import Queue
from wherefore.DataSource import DataSource
from wherefore.Message import Message
from copy import copy


CHECK_FOR_MSG_INTERVAL = 500


class PartitionOffset(Enum):
    NEVER = auto()
    BEGINNING = auto()
    END = auto()


def thread_function(consumer: KafkaConsumer, stop: Union[datetime, int], in_queue: Queue, out_queue: Queue):
    known_sources: Dict[bytes, DataSource] = {}
    start_time = datetime.now()
    while True:
        messages_ctr = 0
        for kafka_msg in consumer:
            new_msg = Message(kafka_msg)
            if not new_msg.source_hash in known_sources:
                known_sources[new_msg.source_hash] = DataSource(new_msg.source_name, new_msg.message_type, start_time)
            known_sources[new_msg.source_hash].process_message(new_msg)
            messages_ctr += 1
            if messages_ctr == CHECK_FOR_MSG_INTERVAL:
                break
        if not in_queue.empty():
            new_msg = in_queue.get()
            if new_msg == "exit":
                break
            elif new_msg == "get_update":
                out_queue.put(copy(known_sources))
    consumer.close(True)


class KafkaMessageTracker:
    def __init__(self, broker: str, topic: str, partition: int = -1, start: Union[int, datetime, PartitionOffset] = PartitionOffset.END, stop: Union[int, datetime, PartitionOffset] = PartitionOffset.NEVER):
        consumer = KafkaConsumer(bootstrap_servers=broker, fetch_max_bytes=52428800 * 6, consumer_timeout_ms=100)
        existing_topics = consumer.topics()
        if topic not in existing_topics:
            raise RuntimeError(f"Topic \"{topic}\" does not exist.")
        existing_partitions = consumer.partitions_for_topic(topic)
        if partition == -1:
            partition = existing_partitions.pop()
        elif partition not in existing_partitions:
            raise RuntimeError(f"Partition {partition} for topic \"{topic}\" does not exist.")
        topic_partition = TopicPartition(topic, partition)
        consumer.assign([topic_partition, ])
        if start == PartitionOffset.BEGINNING:
            consumer.seek_to_beginning()
        elif start == PartitionOffset.END or start == PartitionOffset.NEVER:
            consumer.seek_to_end()
        elif type(start) is int:
            consumer.seek(partitions=topic_partition, offset=start)
        elif type(start) is datetime:
            found_offsets = consumer.offsets_for_times({topic: int(start.timestamp() * 1000)})
            consumer.seek(partition=topic_partition, offset=found_offsets[topic])
        self.to_thread = Queue()
        self.from_thread = Queue()
        self.thread = Thread(target=thread_function, daemon=True, kwargs={"consumer":consumer, "stop":stop, "in_queue":self.to_thread, "out_queue":self.from_thread})
        self.thread.start()

    def get_latest_values(self):
        self.to_thread.put("get_update")
        return self.from_thread.get(block=True)



