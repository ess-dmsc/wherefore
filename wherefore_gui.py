import PyQt5.QtWidgets as QtWidgets
from PyQt5.QtCore import QSettings, QTimer
import PyQt5.uic
from PyQt5 import QtGui
from pathlib import Path
from typing import Optional, Union
from led import Led
from concurrent.futures import ThreadPoolExecutor
from wherefore.KafkaTopicPartitions import get_topic_partitions
from wherefore.KafkaMessageTracker import PartitionOffset
from wherefore.TopicPartitionSourceTreeModel import TopicPartitionSourceTreeModel
from PyQt5.QtCore import QSettings
from wherefore.TreeItems import PartitionItem, SourceItem
from datetime import datetime, timezone, timedelta


def datetime_to_str(timestamp: Optional[datetime], now: datetime):
    if timestamp is None:
        return "n/a"
    time_str = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f %Z")
    time_diff = (now - timestamp).total_seconds()
    use_diff = time_diff
    diff_unit = "s"
    if abs(time_diff) > 59:
        use_diff = time_diff / 60
        diff_unit = "m"
    if abs(time_diff) > 3599:
        use_diff = time_diff / 3600
        diff_unit = "h"
    if abs(time_diff) > 3600 * 24 - 1:
        use_diff = time_diff / (3600 * 24)
        diff_unit = "d"
    return f"{time_str}, age: {use_diff:.2f} {diff_unit}"


class SourceViewerApp(QtWidgets.QMainWindow):
    UpdateRates = [10, 20, 50]
    SingleShot = False

    def __init__(self):
        # Parent constructor
        super(SourceViewerApp, self).__init__()
        self.ui = None
        self.brokerEditTimer = QTimer()
        self.brokerEditTimer.timeout.connect(self.onBrokerEditTimer)
        self.brokerEditTimer.setSingleShot(True)

        self.editStartStopTimer = QTimer()
        self.editStartStopTimer.timeout.connect(self.onEditStartStopTimer)
        self.editStartStopTimer.setSingleShot(True)

        self.thread_pool = ThreadPoolExecutor(5)
        self.topicPartitionQueryTimer = QTimer()
        self.topicPartitionQueryTimer.timeout.connect(self.onCheckIfTopicsUpdated)
        self.topicPartitionQueryTimer.start(500)

        self.sourceTrackerTimer = QTimer()
        self.sourceTrackerTimer.timeout.connect(self.onCheckForNewSources)
        self.sourceTrackerTimer.start(500)

        self.selectedSource: Optional[SourceItem] = None
        self.selectedPartition: Optional[PartitionItem] = None
        self.updateDataTimer = QTimer()
        self.updateDataTimer.timeout.connect(self.onUpdateSelectedData)
        self.updateDataTimer.start(100)

        self.topicUpdateFuture = None
        self.topicPartitionModel = TopicPartitionSourceTreeModel()
        self.config = QSettings("ESS", "Wherefore")
        self.setup()
        self.current_start: Union[
            int, datetime, PartitionOffset
        ] = self.getStartCondition()
        self.current_stop: Union[
            int, datetime, PartitionOffset
        ] = self.getStopCondition()

    def setup(self):
        import WhereforeGUI

        self.ui = WhereforeGUI.Ui_MainWindow()
        self.ui.setupUi(self)

        self.ui.startAtSelector.addItems(["beginning", "end", "offset", "timestamp"])
        self.ui.startAtSelector.setCurrentIndex(1)
        self.ui.endAtSelector.addItems(["never", "end", "offset", "timestamp"])
        self.ui.enableDefaultComboBox.addItems(["Enable new", "Disable new"])
        self.ui.startTimeEdit.hide()
        self.ui.startOffsetEdit.hide()
        self.ui.endOffsetEdit.hide()
        self.ui.endTimeEdit.hide()
        self.ui.startAtSelector.currentIndexChanged.connect(self.on_change_start_at)
        self.ui.endAtSelector.currentIndexChanged.connect(self.on_change_end_at)
        self.ui.brokerLed = Led(self)
        self.ui.consumerBarLayout.insertWidget(1, self.ui.brokerLed)
        self.ui.brokerAddressEdit.textEdited.connect(self.startTextEditedTimer)
        self.ui.enableAllButton.clicked.connect(self.onEnableAllPartitions)
        self.ui.disableAllButton.clicked.connect(self.onDisableAllPartitions)
        self.ui.topicPartitionSourceTree.setModel(self.topicPartitionModel)
        self.ui.topicPartitionSourceTree.setDragEnabled(True)
        self.ui.startOffsetEdit.textEdited.connect(self.onRestartStartStopTimer)
        self.ui.endOffsetEdit.textEdited.connect(self.onRestartStartStopTimer)
        self.ui.startTimeEdit.editingFinished.connect(self.onRestartStartStopTimer)
        self.ui.endTimeEdit.editingFinished.connect(self.onRestartStartStopTimer)
        header_view = self.ui.topicPartitionSourceTree.header()
        header_view.setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)
        header_view.setStretchLastSection(False)
        header_view.resizeSection(1, 50)
        self.ui.topicPartitionSourceTree.setHeader(header_view)

        self.ui.topicPartitionSourceTree.selectionModel().selectionChanged.connect(
            self.on_tree_node_selection
        )

        self.ui.brokerAddressEdit.setText(self.config.value("kafka_address", type=str))
        if len(self.ui.brokerAddressEdit.text()) > 0:
            self.onBrokerEditTimer()

        self.ui.enableDefaultComboBox.setCurrentIndex(
            self.config.value("enable_default", type=int, defaultValue=0)
        )
        self.show()

    def on_change_end_at(self, new_index):
        if new_index == 0 or new_index == 1:
            self.ui.endOffsetEdit.hide()
            self.ui.endTimeEdit.hide()
        elif new_index == 2:
            self.ui.endOffsetEdit.show()
            self.ui.endTimeEdit.hide()
        elif new_index == 3:
            self.ui.endOffsetEdit.hide()
            self.ui.endTimeEdit.show()
        self.onRestartStartStopTimer()

    def on_change_start_at(self, new_index):
        if new_index == 0 or new_index == 1:
            self.ui.startOffsetEdit.hide()
            self.ui.startTimeEdit.hide()
        elif new_index == 2:
            self.ui.startOffsetEdit.show()
            self.ui.startTimeEdit.hide()
        elif new_index == 3:
            self.ui.startOffsetEdit.hide()
            self.ui.startTimeEdit.show()
        self.onRestartStartStopTimer()

    def on_tree_node_selection(self, newSelection, oldSelection):
        if len(newSelection) == 0:
            self.selectedSource = None
            self.selectedPartition = None
            return
        selected_item = newSelection.first().indexes()[0].internalPointer()
        if isinstance(selected_item, PartitionItem):
            self.selectedPartition = selected_item
            self.selectedSource = None
        elif isinstance(selected_item, SourceItem):
            self.selectedPartition = selected_item.parent
            self.selectedSource = selected_item
            self.ui.sourceNameValue.setText(self.selectedSource.name)
            self.ui.sourceTypeValue.setText(self.selectedSource.type)
        else:
            self.selectedPartition = None
            self.selectedSource = None
            self.ui.sourceNameValue.setText("n/a")
            self.ui.sourceTypeValue.setText("n/a")

    def onUpdateSelectedData(self):
        if self.selectedPartition is not None and self.selectedPartition.enabled:
            partition_info = self.selectedPartition.get_partition_info()
            if partition_info is None:
                return
            self.ui.lowOffsetValue.setText(str(partition_info.low))
            self.ui.highOffsetValue.setText(str(partition_info.high))
            self.ui.lagValue.setText(str(partition_info.lag))
        else:
            self.ui.lowOffsetValue.setText("n/a")
            self.ui.highOffsetValue.setText("n/a")
            self.ui.lagValue.setText("n/a")
        if self.selectedSource is not None:
            current_source_info = self.selectedSource.parent.get_known_sources()
            if current_source_info is None:
                self.unset_source_info()
                return
            for c_source in current_source_info.values():
                if (
                    c_source.source_name == self.selectedSource.name
                    and c_source.source_type == self.selectedSource.type
                ):
                    now = datetime.now(tz=timezone.utc)
                    self.ui.firstMsgTimeValue.setText(
                        datetime_to_str(c_source.first_timestamp, now)
                    )
                    self.ui.lastMsgKafkaTimeValue.setText(
                        datetime_to_str(c_source.last_message.kafka_timestamp, now)
                    )
                    self.ui.lastMsgReceiveTimeValue.setText(
                        datetime_to_str(c_source.last_message.local_timestamp, now)
                    )
                    self.ui.lastMsgTimeValue.setText(
                        datetime_to_str(c_source.last_message.timestamp, now)
                    )

                    self.ui.consumptionRateValue.setText(
                        f"{c_source.processed_per_second:.2f}/s"
                    )
                    self.ui.currentMsgSizeValue.setText(
                        f"{c_source.last_message.size:.0f} bytes"
                    )
                    self.ui.dataRateValue.setText(
                        f"{c_source.bytes_per_second:.0f} bytes/s"
                    )
                    self.ui.dataValue.setText(c_source.last_message.data)
                    self.ui.currentOffsetValue.setText(
                        f"{c_source.last_message.offset}"
                    )
                    self.ui.firstOffsetValue.setText(f"{c_source.first_offset}")
                    self.ui.messageRateValue.setText(
                        f"{c_source.messages_per_second:.2f}/s"
                    )
                    self.ui.receivedMessagesValue.setText(
                        f"{c_source.processed_messages}"
                    )
                    break
        else:
            self.unset_source_info()

    def unset_source_info(self):
        self.ui.firstMsgTimeValue.setText("n/a")
        self.ui.lastMsgKafkaTimeValue.setText("n/a")
        self.ui.lastMsgReceiveTimeValue.setText("n/a")
        self.ui.lastMsgTimeValue.setText("n/a")
        self.ui.currentMsgSizeValue.setText("n/a")
        self.ui.dataRateValue.setText("n/a")
        self.ui.dataValue.setText("n/a")

        self.ui.consumptionRateValue.setText("n/a")
        self.ui.currentOffsetValue.setText("n/a")
        self.ui.firstOffsetValue.setText("n/a")
        self.ui.messageRateValue.setText("n/a")
        self.ui.receivedMessagesValue.setText("n/a")

    def startTextEditedTimer(self):
        self.brokerEditTimer.start(500)

    def onBrokerEditTimer(self):
        self.topicPartitionModel.set_kafka_broker(self.ui.brokerAddressEdit.text())
        self.topicUpdateFuture = self.thread_pool.submit(
            get_topic_partitions, self.ui.brokerAddressEdit.text()
        )

    def onCheckForNewSources(self):
        self.topicPartitionModel.check_for_sources()

    def onEnableAllPartitions(self):
        self.topicPartitionModel.enable_all()

    def onDisableAllPartitions(self):
        self.topicPartitionModel.disable_all()

    def onCheckIfTopicsUpdated(self):
        if self.topicUpdateFuture is not None and self.topicUpdateFuture.done():
            try:
                result = self.topicUpdateFuture.result()
                if result is None:
                    self.ui.brokerLed.turn_off()
                else:
                    self.ui.brokerLed.turn_on()
                    self.updateTopicTree(result)
                self.topicUpdateFuture = self.thread_pool.submit(
                    get_topic_partitions, self.ui.brokerAddressEdit.text()
                )
            except ValueError:
                pass  # Ignore

    def updateTopicTree(self, known_topics):
        enable_new_partitions = True
        if self.ui.enableDefaultComboBox.currentIndex() == 1:
            enable_new_partitions = False
        self.topicPartitionModel.update_topics(
            known_topics, self.current_start, self.current_stop, enable_new_partitions
        )

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        self.config.setValue("kafka_address", self.ui.brokerAddressEdit.text())
        self.config.setValue(
            "enable_default", self.ui.enableDefaultComboBox.currentIndex()
        )
        event.accept()

    def onRestartStartStopTimer(self):
        self.editStartStopTimer.start(500)

    def onEditStartStopTimer(self):
        new_start = self.getStartCondition()
        if self.current_start != new_start:
            self.topicPartitionModel.setNewStart(new_start)
            self.current_start = new_start

        new_stop = self.getStopCondition()
        if self.current_stop != new_stop:
            self.topicPartitionModel.setNewStop(new_stop)
            self.current_start = new_stop

    def getStartCondition(self):
        if self.ui.startAtSelector.currentIndex() == 0:
            return PartitionOffset.BEGINNING
        elif self.ui.startAtSelector.currentIndex() == 1:
            return PartitionOffset.END
        elif self.ui.startAtSelector.currentIndex() == 2:
            return_int = 0
            try:
                return_int = int(self.ui.startOffsetEdit.text())
            except ValueError:
                pass
            return return_int
        elif self.ui.startAtSelector.currentIndex() == 3:
            return self.ui.startTimeEdit.dateTime()

    def getStopCondition(self):
        if self.ui.endAtSelector.currentIndex() == 0:
            return PartitionOffset.NEVER
        elif self.ui.endAtSelector.currentIndex() == 1:
            return PartitionOffset.END
        elif self.ui.endAtSelector.currentIndex() == 2:
            return_int = 0
            try:
                return_int = int(self.ui.endOffsetEdit.text())
            except ValueError:
                pass
            return return_int
        elif self.ui.endAtSelector.currentIndex() == 3:
            return self.ui.endTimeEdit.dateTime()


if __name__ == "__main__":
    # Recompile ui
    ui_file_path = Path("WhereforeGUI.ui")
    if ui_file_path.exists():
        with open(str(ui_file_path)) as ui_file:
            with open("WhereforeGUI.py", "w") as py_ui_file:
                PyQt5.uic.compileUi(ui_file, py_ui_file)

    app = QtWidgets.QApplication([])
    main_window = SourceViewerApp()
    main_window.setup()
    app.exec_()
