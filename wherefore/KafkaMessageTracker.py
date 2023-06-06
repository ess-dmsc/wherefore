from threading import Thread
from typing import Union, Dict, Optional, Tuple, List
from datetime import datetime, timedelta, timezone
from enum import Enum, auto

from queue import Queue, Empty, Full
from wherefore.DataSource import DataSource
from wherefore.KafkaConsumer import KafkaConsumer, make_kafka_consumer
from copy import copy


MAX_MSGS_TO_CONSUME = 500
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
    topic: str,
    partition: int,
    schemas: Optional[List[str]] = None,
    source_name: Optional[str] = None,
):
    known_sources: Dict[bytes, DataSource] = {}
    start_time = datetime.now(tz=timezone.utc)
    update_timer = datetime.now(tz=timezone.utc)
    while True:
        for kafka_msg in consumer.consume(MAX_MSGS_TO_CONSUME):
            if schemas:
                if kafka_msg.message_type not in schemas:
                    continue
            if source_name:
                if kafka_msg.source_name != source_name:
                    continue
            if type(stop) is int and kafka_msg.offset > stop:
                continue
            if (
                type(stop) is datetime
                and kafka_msg.timestamp is not None
                and kafka_msg.timestamp > stop
            ):
                continue
            if (
                type(stop) is datetime
                and kafka_msg.timestamp is None
                and kafka_msg.kafka_timestamp > stop
            ):
                continue
            if kafka_msg.source_hash not in known_sources:
                known_sources[kafka_msg.source_hash] = DataSource(
                    kafka_msg.source_name, kafka_msg.message_type, start_time
                )
            known_sources[kafka_msg.source_hash].process_message(kafka_msg)
        if not in_queue.empty():
            kafka_msg = in_queue.get()
            if kafka_msg == "exit":
                break
        now = datetime.now(tz=timezone.utc)
        if now - update_timer > UPDATE_STATUS_INTERVAL:
            update_timer = now
            try:
                out_queue.put(copy(known_sources), block=False)
                low_offset = consumer.beginning_offset(topic, partition)
                high_offset = consumer.end_offset(topic, partition)
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

        consumer = make_kafka_consumer(broker, security_config)
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
        consumer.assign(
            topic,
            [
                partition,
            ],
        )
        first_offset = consumer.beginning_offset(topic, partition)
        last_offset = consumer.end_offset(topic, partition)
        origin_offset = None
        offset_to_offset = start[1]
        if start[0] == PartitionOffset.BEGINNING:
            origin_offset = first_offset
        elif start[0] == PartitionOffset.END or start == PartitionOffset.NEVER:
            origin_offset = last_offset
        elif type(start[0]) is int:
            if first_offset > start[0]:
                origin_offset = first_offset
            elif last_offset < start[0]:
                origin_offset = last_offset
            else:
                origin_offset = start[0]
        elif type(start[0]) is datetime:
            offsets = consumer.offsets_for_times(
                {partition: int(start[0].timestamp() * 1000)}
            )
            if offsets[partition] is None:
                origin_offset = last_offset
            else:
                origin_offset = offsets[partition]
        else:
            raise RuntimeError("Unknown start offset configured.")

        if offset_to_offset is not None:
            origin_offset += offset_to_offset
            if origin_offset < first_offset:
                origin_offset = first_offset
            elif origin_offset > last_offset:
                origin_offset = last_offset
        consumer.seek(partition, origin_offset)
        self.thread = Thread(
            target=thread_function,
            daemon=True,
            kwargs={
                "consumer": consumer,
                "stop": stop,
                "in_queue": self.to_thread,
                "out_queue": self.from_thread,
                "stop": stop,
                "topic": topic,
                "partition": partition,
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
