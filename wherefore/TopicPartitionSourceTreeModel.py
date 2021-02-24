from PyQt5.QtCore import QAbstractItemModel, QModelIndex, Qt
import typing
from wherefore.TreeItems import RootItem


class TopicPartitionSourceTreeModel(QAbstractItemModel):
    def __init__(self, kafka_broker: typing.Optional[str] = None):
        super().__init__()
        self.kafka_broker = kafka_broker
        self.root_item = RootItem()

    def set_kafka_broker(self, kafka_broker: str):
        self.kafka_broker = kafka_broker

    def update_topics(self, known_topics):
        for c_topic in known_topics:
            if not self.root_item.topic_is_known(c_topic["name"]):
                insert_loc = self.root_item.get_topic_insert_location(c_topic["name"])
                self.beginInsertRows(QModelIndex(), insert_loc, insert_loc)
                self.root_item.add_topic(c_topic["name"])
                self.endInsertRows()
            topic_item = self.root_item.get_topic(c_topic["name"])
            for c_partition in c_topic["partitions"]:
                if not topic_item.partition_is_known(c_partition):
                    insert_loc = topic_item.get_partition_insert_location(c_partition)
                    self.beginInsertRows(self.index(self.root_item.get_topic_location(topic_item.name), 0, QModelIndex()), insert_loc, insert_loc)
                    topic_item.add_partition(c_partition, self.kafka_broker)
                    self.endInsertRows()

    def check_for_sources(self):
        root_index = QModelIndex()
        for i, c_topic in enumerate(self.root_item.topics):
            topic_index = self.index(i, 0, root_index)
            for j, c_partition in enumerate(c_topic.partitions):
                partition_index = self.index(j, 0, topic_index)
                known_sources = c_partition.get_known_sources()
                if known_sources is None:
                    continue
                for source in known_sources.values():
                    if not c_partition.source_is_known(source.source_name, source.source_type):
                        source_location = c_partition.get_source_insert_location(source.source_name, source.source_type)
                        self.beginInsertRows(partition_index, source_location, source_location)
                        c_partition.add_source(source.source_name, source.source_type)
                        self.endInsertRows()


    def columnCount(self, parent: QModelIndex = ...) -> int:
        if parent.isValid():
            return parent.internalPointer().column_count
        else:
            return self.root_item.column_count

    def data(self, index: QModelIndex, role: int = ...) -> typing.Any:
        if not index.isValid():
            return None

        if role != Qt.DisplayRole:
            return None

        item = index.internalPointer()

        return item.data(index.column())

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        if not index.isValid():
            return Qt.NoItemFlags

        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def rowCount(self, parent: QModelIndex = ...) -> int:
        if parent.column() > 0:
            return 0

        if not parent.isValid():
            parent_item = self.root_item
        else:
            parent_item = parent.internalPointer()

        return parent_item.child_count

    def parent(self, child: QModelIndex) -> QModelIndex:
        if not child.isValid():
            return QModelIndex()

        child_item = child.internalPointer()
        parent_item = child_item.parent

        if parent_item == self.root_item:
            return QModelIndex()

        return self.createIndex(parent_item.row, 0, parent_item)

    def index(self, row: int, column: int, parent: QModelIndex = ...) -> QModelIndex:
        if not self.hasIndex(row, column, parent):
            return QModelIndex()

        if not parent.isValid():
            parent_item = self.root_item
        else:
            parent_item = parent.internalPointer()

        child_item = parent_item.child(row)
        if child_item:
            return self.createIndex(row, column, child_item)
        else:
            return QModelIndex()

    def headerData(self, section, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.root_item.data(section)

        return None
