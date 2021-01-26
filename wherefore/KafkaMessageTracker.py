from threading import Thread
from typing import Union, Dict
from datetime import datetime, timedelta
from enum import Enum, auto
from kafka import KafkaConsumer, TopicPartition
from queue import Queue, Empty, Full
from wherefore.DataSource import DataSource
from wherefore.Message import Message
from copy import copy


CHECK_FOR_MSG_INTERVAL = 500
UPDATE_STATUS_INTERVAL = timedelta(milliseconds=50)


class PartitionOffset(Enum):
    NEVER = auto()
    BEGINNING = auto()
    END = auto()


class HighLowOffset:
    def __init__(self, low, high, lag = -1):
        self.low = low
        self.lag = lag
        self.high = high


def thread_function(consumer: KafkaConsumer, stop: Union[datetime, int], in_queue: Queue, out_queue: Queue, topic_partition):
    known_sources: Dict[bytes, DataSource] = {}
    start_time = datetime.now()
    update_timer = datetime.now()
    while True:
        messages_ctr = 0
        for kafka_msg in consumer:
            new_msg = Message(kafka_msg)
            if type(stop) is int and new_msg.offset > stop:
                pass
            elif type(stop) is datetime and new_msg.timestamp is not None and new_msg.timestamp > stop:
                pass
            elif type(stop) is datetime and new_msg.timestamp is None and new_msg.kafka_timestamp > stop:
                pass
            else:
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
        if datetime.now() - update_timer > UPDATE_STATUS_INTERVAL:
            update_timer = datetime.now()
            try:
                out_queue.put(copy(known_sources), block=False)
                low_offset = consumer.beginning_offsets([topic_partition, ])[topic_partition]
                high_offset = consumer.end_offsets([topic_partition, ])[topic_partition]
                out_queue.put(HighLowOffset(low_offset, high_offset))
            except Full:
                pass  # Do nothing
    consumer.close(True)


class KafkaMessageTracker:
    def __init__(self, broker: str, topic: str, partition: int = -1, start: Union[int, datetime, PartitionOffset] = PartitionOffset.END, stop: Union[int, datetime, PartitionOffset] = PartitionOffset.NEVER):
        consumer = KafkaConsumer(bootstrap_servers=broker, fetch_max_bytes=52428800 * 6, consumer_timeout_ms=100)
        existing_topics = consumer.topics()
        self.current_msg = None
        self.current_offset_limits = HighLowOffset(-1, -1)
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
            first_offset = consumer.beginning_offsets([topic_partition, ])
            if first_offset[topic_partition] > start:
                consumer.seek_to_beginning()
            else:
                consumer.seek(partition=topic_partition, offset=start)
        elif type(start) is datetime:
            found_offsets = consumer.offsets_for_times({topic_partition: int(start.timestamp() * 1000)})
            consumer.seek(partition=topic_partition, offset=found_offsets[topic_partition].offset)
        self.to_thread = Queue()
        self.from_thread = Queue(maxsize=100)
        self.thread = Thread(target=thread_function, daemon=True, kwargs={"consumer":consumer, "stop":stop, "in_queue":self.to_thread, "out_queue":self.from_thread, "stop":stop, "topic_partition":topic_partition})
        self.thread.start()

    def get_messages(self):
        while not self.from_thread.empty():
            try:
                current_msg = self.from_thread.get(block=False)
                if type(current_msg) is dict:
                    self.current_msg = current_msg
                elif type(current_msg) is HighLowOffset:
                    self.current_offset_limits = current_msg
            except Empty:
                return

    def get_latest_values(self):
        self.get_messages()
        return self.current_msg

    def get_current_edge_offsets(self):
        self.get_messages()
        return self.current_offset_limits



