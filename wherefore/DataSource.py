from datetime import datetime
from wherefore.Message import Message
from datetime import datetime


class DataSource:
    def __init__(self, source_name: str, source_type: str, start_time: datetime):
        self._source_name = source_name
        self._source_type = source_type
        self._processed_messages = 0
        self._start_time = start_time

    def process_message(self, msg: Message):
        pass

    @property
    def source_name(self) -> str:
        return self._source_name

    @property
    def source_type(self) -> str:
        return self._source_type

    @property
    def processed_messages(self) -> int:
        return self._processed_messages

    @property
    def messages_per_second(self) -> float:
        time_diff = datetime.now() - self._start_time
        return self._processed_messages / time_diff.total_seconds()


