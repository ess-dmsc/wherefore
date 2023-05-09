from threading import Thread
from typing import Union, Dict, Optional, Tuple, List
from datetime import datetime, timedelta, timezone
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
    def __init__(self, low, high, lag=-1):
        self.low = low
        self.lag = lag
        self.high = high


def thread_function(
    consumer: KafkaConsumer,
    stop: Union[datetime, int],
    in_queue: Queue,
    out_queue: Queue,
    topic_partition,
    schemas: Optional[List[str]] = None,
    source_name: Optional[str] = None,
):
    known_sources: Dict[bytes, DataSource] = {}
    start_time = datetime.now(tz=timezone.utc)
    update_timer = datetime.now(tz=timezone.utc)
    while True:
        messages_ctr = 0
        for kafka_msg in consumer:
            if messages_ctr == CHECK_FOR_MSG_INTERVAL:
                break
            messages_ctr += 1
            new_msg = Message(kafka_msg)
            if schemas:
                if new_msg.message_type not in schemas:
                    continue
            if source_name:
                if new_msg.source_name != source_name:
                    continue
            if type(stop) is int and new_msg.offset > stop:
                continue
            if (
                type(stop) is datetime
                and new_msg.timestamp is not None
                and new_msg.timestamp > stop
            ):
                continue
            if (
                type(stop) is datetime
                and new_msg.timestamp is None
                and new_msg.kafka_timestamp > stop
            ):
                continue
            if not new_msg.source_hash in known_sources:
                known_sources[new_msg.source_hash] = DataSource(
                    new_msg.source_name, new_msg.message_type, start_time
                )
            known_sources[new_msg.source_hash].process_message(new_msg)
        if not in_queue.empty():
            new_msg = in_queue.get()
            if new_msg == "exit":
                break
        now = datetime.now(tz=timezone.utc)
        if now - update_timer > UPDATE_STATUS_INTERVAL:
            update_timer = now
            try:
                out_queue.put(copy(known_sources), block=False)
                low_offset = consumer.beginning_offsets(
                    [
                        topic_partition,
                    ]
                )[topic_partition]
                high_offset = consumer.end_offsets(
                    [
                        topic_partition,
                    ]
                )[topic_partition]
                out_queue.put(HighLowOffset(low_offset, high_offset))
            except Full:
                pass  # Do nothing
    consumer.close(True)


class KafkaMessageTracker:
    def __init__(
        self,
        broker: str,
        topic: str,
        partition: int = -1,
        start: Tuple[
            Union[int, datetime, PartitionOffset], Optional[int]
        ] = PartitionOffset.END,
        stop: Union[int, datetime, PartitionOffset] = PartitionOffset.NEVER,
        security_config: Dict[str, str] = None,
        schemas: Optional[List[str]] = None,
        source_name: Optional[str] = None,
    ):
        self.to_thread = Queue()
        self.from_thread = Queue(maxsize=100)

        if security_config is None:
            security_config = {}

        consumer = KafkaConsumer(
            bootstrap_servers=broker,
            fetch_max_bytes=52428800 * 6,
            consumer_timeout_ms=100,
            **security_config
        )
        existing_topics = consumer.topics()
        self.current_msg = None
        self.current_offset_limits = HighLowOffset(-1, -1)
        if topic not in existing_topics:
            raise RuntimeError(f'Topic "{topic}" does not exist.')
        existing_partitions = consumer.partitions_for_topic(topic)
        if partition == -1:
            partition = existing_partitions.pop()
        elif partition not in existing_partitions:
            raise RuntimeError(
                f'Partition {partition} for topic "{topic}" does not exist.'
            )
        topic_partition = TopicPartition(topic, partition)
        consumer.assign(
            [
                topic_partition,
            ]
        )
        first_offset = consumer.beginning_offsets([topic_partition])[topic_partition]
        last_offset = consumer.end_offsets([topic_partition])[topic_partition]
        origin_offset = None
        offset_to_offset = start[1]
        if start[0] == PartitionOffset.BEGINNING:
            origin_offset = first_offset
            # consumer.seek_to_beginning()
            # if type(start[1]) == int and start[1] > 0 and first_offset + start[1] <= last_offset:
            #     consumer.seek(partition=topic_partition, offset=first_offset + start[1])
        elif start[0] == PartitionOffset.END or start == PartitionOffset.NEVER:
            origin_offset = last_offset
            # consumer.seek_to_end()
            # if type(start[1]) == int and start[1] < 0 and last_offset + start[1] >= first_offset:
            #     consumer.seek(partition=topic_partition, offset=first_offset + start[1])
        elif type(start[0]) is int:
            if first_offset > start[0]:
                origin_offset = first_offset
                # consumer.seek_to_beginning()
            elif last_offset < start[0]:
                origin_offset = last_offset
            else:
                origin_offset = start[0]
            #     consumer.seek_to_end()
            # else:
            #     consumer.seek(partition=topic_partition, offset=start[0])
        elif type(start[0]) is datetime:
            found_offsets = consumer.offsets_for_times(
                {topic_partition: int(start[0].timestamp() * 1000)}
            )
            if found_offsets[topic_partition] is None:
                origin_offset = last_offset
            else:
                origin_offset = found_offsets[topic_partition].offset

            # if type(start[1]) == int:
            #     used_offset += start[1]
            # consumer.seek(partition=topic_partition, offset=used_offset)
        else:
            raise RuntimeError("Unknown start offset configured.")

        if offset_to_offset is not None:
            origin_offset += offset_to_offset
            if origin_offset < first_offset:
                origin_offset = first_offset
            elif origin_offset > last_offset:
                origin_offset = last_offset
        consumer.seek(partition=topic_partition, offset=origin_offset)
        self.thread = Thread(
            target=thread_function,
            daemon=True,
            kwargs={
                "consumer": consumer,
                "stop": stop,
                "in_queue": self.to_thread,
                "out_queue": self.from_thread,
                "stop": stop,
                "topic_partition": topic_partition,
                "schemas": schemas,
                "source_name": source_name,
            },
        )
        self.thread.start()

    def stop_thread(self):
        self.to_thread.put("exit")

    def __del__(self):
        self.stop_thread()

    def _get_messages(self):
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
        self._get_messages()
        return self.current_msg

    def get_current_edge_offsets(self) -> HighLowOffset:
        self._get_messages()
        return self.current_offset_limits
