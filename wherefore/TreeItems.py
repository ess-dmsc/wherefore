from typing import List, Optional, Dict, Union
from wherefore.KafkaMessageTracker import KafkaMessageTracker, PartitionOffset
from concurrent.futures import ThreadPoolExecutor, Future
from kafka.errors import NoBrokersAvailable
from wherefore.DataSource import DataSource
from datetime import datetime


class RootItem:
    def __init__(self):
        super().__init__()
        self._topics: List[TopicItem] = []

    def topic_is_known(self, topic_name) -> bool:
        for c_topic in self._topics:
            if c_topic.name == topic_name:
                return True
        return False

    def get_topic_insert_location(self, topic_name) -> int:
        for i, c_topic in enumerate(self._topics):
            if c_topic.name.lower() > topic_name.lower():
                return i
        return len(self._topics)

    def get_topic_location(self, topic_name) -> int:
        for i, c_topic in enumerate(self._topics):
            if c_topic.name == topic_name:
                return i

    def add_topic(self, topic_name: str):
        self._topics.insert(
            self.get_topic_insert_location(topic_name), TopicItem(topic_name, self)
        )

    def get_topic(self, topic_name) -> "TopicItem":
        for c_topic in self._topics:
            if c_topic.name == topic_name:
                return c_topic

    def child(self, row: int) -> "TopicItem":
        return self._topics[row]

    @property
    def child_count(self) -> int:
        return len(self._topics)

    @property
    def parent(self) -> None:
        return None

    @property
    def column_count(self) -> int:
        return 2

    def data(self, column: int) -> str:
        try:
            return ("", "Enabled")[column]
        except IndexError:
            return None

    @property
    def topics(self) -> List["TopicItem"]:
        return self._topics

    @property
    def row(self) -> int:
        return 0

    def index_of(self, item: "TopicItem") -> int:
        return self._topics.index(item)


class TopicItem:
    def __init__(self, topic_name: str, parent: RootItem):
        super().__init__()
        self._topic_name = topic_name
        self._partitions: List[PartitionItem] = []
        self._parent = parent

    def partition_is_known(self, partition_id: int) -> bool:
        for c_partition in self._partitions:
            if c_partition.partition_id == partition_id:
                return True
        return False

    def get_partition_insert_location(self, partition_id) -> int:
        for i, c_partition in enumerate(self._partitions):
            if c_partition.partition_id > partition_id:
                return i
        return len(self._partitions)

    def add_partition(
        self,
        partition: int,
        broker: str,
        start: Union[int, datetime, PartitionOffset],
        stop: Union[int, datetime, PartitionOffset],
        security_config: Dict[str, str],
        enable: bool = True,
    ):
        self._partitions.insert(
            self.get_partition_insert_location(partition),
            PartitionItem(partition, self, broker, start, stop, security_config, enable),
        )

    def child(self, row: int) -> "PartitionItem":
        return self._partitions[row]

    @property
    def child_count(self) -> int:
        return len(self._partitions)

    @property
    def parent(self) -> RootItem:
        return self._parent

    @property
    def column_count(self) -> int:
        return 2

    def data(self, column: int):
        return self._topic_name

    @property
    def name(self) -> str:
        return self._topic_name

    @property
    def partitions(self) -> List["PartitionItem"]:
        return self._partitions

    @property
    def row(self) -> int:
        return self._parent.index_of(self)

    def index_of(self, item: "PartitionItem") -> int:
        return self._partitions.index(item)


class PartitionItem:
    def __init__(
        self,
        partition: int,
        parent: TopicItem,
        broker: str,
        start: Union[int, datetime, PartitionOffset] = PartitionOffset.END,
        stop: Union[int, datetime, PartitionOffset] = PartitionOffset.NEVER,
        security_config: Dict[str, str] = None,
        enable: bool = True,
    ):
        super().__init__()
        self._enabled = enable
        self._partition = partition
        self._sources: List[SourceItem] = []
        self._parent = parent
        self._broker = broker
        self._thread_pool = ThreadPoolExecutor(2)
        self._message_tracker: Optional[KafkaMessageTracker] = None
        self._message_tracker_future: Optional[Future] = None
        self._start = start
        self._stop = stop
        if security_config is None:
            self._security_config = {}
        else:
            self._security_config = security_config
        if enable:
            self.start_message_monitoring()

    def _re_start_message_monitoring(self):
        if self._enabled:
            if self._message_tracker is not None:
                self._message_tracker.stop_thread()
                self._message_tracker = None
            if self._message_tracker_future is not None:
                if self._message_tracker_future.done():
                    try:
                        self._message_tracker_future.result().stop_thread()
                    except NoBrokersAvailable:
                        pass
                self._message_tracker_future = None
            self.start_message_monitoring()

    def set_new_start(self, start: Union[int, datetime, PartitionOffset]):
        self._start = start
        self._re_start_message_monitoring()

    def set_new_stop(self, stop: Union[int, datetime, PartitionOffset]):
        self._stop = stop
        self._re_start_message_monitoring()

    def start_message_monitoring(self):
        if self._broker is not None and self._broker != "" and self._enabled:
            launch_tracker = lambda broker, topic, partition: KafkaMessageTracker(
                broker, topic, partition, (self._start, None), self._stop, self._security_config
            )
            self._message_tracker_future = self._thread_pool.submit(
                launch_tracker, self._broker, self._parent.name, self.partition_id
            )

    def _check_message_tracker_status(self):
        if (
            self._enabled
            and self._message_tracker_future is not None
            and self._message_tracker_future.done()
        ):
            try:
                self._message_tracker = self._message_tracker_future.result()
                self._message_tracker_future = None
            except NoBrokersAvailable:
                self.start_message_monitoring()
        elif (
            self._enabled
            and self._message_tracker_future is None
            and self._message_tracker is None
        ):
            self.start_message_monitoring()

    def get_known_sources(self) -> Optional[Dict[bytes, DataSource]]:
        self._check_message_tracker_status()
        if self._message_tracker is not None:
            return self._message_tracker.get_latest_values()
        return None

    def get_partition_info(self):
        self._check_message_tracker_status()
        if self._message_tracker is not None:
            return self._message_tracker.get_current_edge_offsets()
        return None

    def source_is_known(self, source_name: str, source_type: str):
        for c_source in self._sources:
            if c_source.name == source_name and c_source.type == source_type:
                return True
        return False

    def get_source_insert_location(self, source_name, source_type) -> int:
        temp_source = SourceItem(source_name, source_type, self)
        for i, c_source in enumerate(self._sources):
            if c_source > temp_source:
                return i
        return len(self._sources)

    def add_source(self, source_name: str, source_type: str, reference_msg: bytes):
        self._sources.insert(
            self.get_source_insert_location(source_name, source_type),
            SourceItem(source_name, source_type, self, reference_msg),
        )

    def child(self, row: int) -> "SourceItem":
        return self._sources[row]

    @property
    def child_count(self) -> int:
        return len(self._sources)

    @property
    def parent(self) -> TopicItem:
        return self._parent

    @property
    def column_count(self) -> int:
        return 2

    def data(self, column: int) -> str:
        return f"Partition #{self._partition}"

    @property
    def row(self) -> int:
        return self._parent.index_of(self)

    def index_of(self, item: "SourceItem") -> int:
        return self._sources.index(item)

    @property
    def partition_id(self) -> int:
        return self._partition

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, new_value: bool):
        if self._enabled and not new_value:
            if (
                self._message_tracker_future is not None
                and self._message_tracker_future.done()
            ):
                # Note, this code will fail to stop the thread in some (corner) cases
                try:
                    self._message_tracker_future.result().stop_thread()
                except NoBrokersAvailable:
                    pass
            self._message_tracker_future = None
            if self._message_tracker is not None:
                self._message_tracker.stop_thread()
                self._message_tracker = None
        elif not self._enabled and new_value:
            self.start_message_monitoring()
        self._enabled = new_value


class SourceItem:
    def __init__(self, source_name: str, source_type: str, parent: PartitionItem, reference_msg: Optional[bytes] = None):
        super().__init__()
        self._source_name = source_name
        self._source_type = source_type
        self._parent = parent
        self._reference_msg = reference_msg

    def __eq__(self, other: "SourceItem"):
        return self.name == other.name and self.type == other.type

    def __lt__(self, other: "SourceItem"):
        if self.name == other.name:
            return self.type < other.type
        return self.name < other.name

    @property
    def child_count(self) -> int:
        return 0

    @property
    def parent(self) -> PartitionItem:
        return self._parent

    @property
    def column_count(self) -> int:
        return 2

    def data(self, column: int):
        return f"{self._source_name} : {self._source_type}"

    @property
    def row(self) -> int:
        return self._parent.index_of(self)

    @property
    def name(self) -> str:
        return self._source_name

    @property
    def type(self) -> str:
        return self._source_type
