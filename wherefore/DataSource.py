from wherefore.Message import Message
from datetime import datetime
from typing import Optional


class DataSource:
    def __init__(self, source_name: str, source_type: str, start_time: datetime):
        self._source_name = source_name
        self._source_type = source_type
        self._processed_messages = 0
        self._start_time = start_time
        self._last_message: Optional[Message] = None
        self._first_offset: Optional[int] = None
        self._first_timestamp: Optional[datetime] = None

    def process_message(self, msg: Message):
        self._processed_messages += 1
        self._last_message = msg
        if self._first_offset is None:
            self._first_offset = msg.offset
            if msg.timestamp is None:
                self._first_timestamp = msg.kafka_timestamp
            else:
                self._first_timestamp = msg.timestamp

    @property
    def source_name(self) -> str:
        return self._source_name

    @property
    def first_offset(self) -> int:
        return self._first_offset

    @property
    def first_timestamp(self) -> datetime:
        return self._first_timestamp

    @property
    def last_timestamp(self) -> datetime:
        if self._last_message.timestamp is None:
            return self._last_message.kafka_timestamp
        return self._last_message.timestamp

    @property
    def source_type(self) -> str:
        return self._source_type

    @property
    def processed_messages(self) -> int:
        return self._processed_messages

    @property
    def processed_per_second(self) -> float:
        time_diff = datetime.now() - self._start_time
        return self._processed_messages / time_diff.total_seconds()

    @property
    def messages_per_second(self) -> float:
        try:
            time_diff = self.last_timestamp - self._first_timestamp
            return self._processed_messages / time_diff.total_seconds()
        except ZeroDivisionError:
            return 0.0

    @property
    def last_message(self):
        return self._last_message

