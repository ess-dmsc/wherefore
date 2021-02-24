import PyQt5.QtWidgets as QtWidgets
from PyQt5.QtCore import QSettings, QTimer
import PyQt5.uic
from PyQt5 import QtGui
from pathlib import Path
from typing import Optional
from led import Led
from concurrent.futures import ThreadPoolExecutor
from wherefore.KafkaTopicPartitions import get_topic_partitions
from wherefore.TopicPartitionSourceTreeModel import TopicPartitionSourceTreeModel
from PyQt5.QtCore import QSettings
from wherefore.TreeItems import PartitionItem, SourceItem


class AdcViewerApp(QtWidgets.QMainWindow):
    UpdateRates = [10, 20, 50]
    SingleShot = False

    def __init__(self):
        # Parent constructor
        super(AdcViewerApp, self).__init__()
        self.ui = None
        self.brokerEditTimer = QTimer()
        self.brokerEditTimer.timeout.connect(self.onBrokerEditTimer)
        self.brokerEditTimer.setSingleShot(True)
        self.thread_pool = ThreadPoolExecutor(5)
        self.topicPartitionQueryTimer = QTimer()
        self.topicPartitionQueryTimer.timeout.connect(self.onCheckIfTopicsUpdated)
        self.topicPartitionQueryTimer.start(500)

        self.sourceTrackerTimer = QTimer()
        self.sourceTrackerTimer.timeout.connect(self.onCheckForNewSources)
        self.sourceTrackerTimer.start(500)

        self.selectedSource: Optional[SourceItem] = None
        self.selectedPartition: Optional[PartitionItem] = None
        # self.sourceTrackerTimer = QTimer()
        # self.sourceTrackerTimer.timeout.connect(self.onUpdateSelectedData)
        # self.sourceTrackerTimer.start(100)

        self.topicUpdateFuture = None
        self.topicPartitionModel = TopicPartitionSourceTreeModel()
        self.config = QSettings("ESS", "Wherefore")
        self.setup()

    def setup(self):
        import WhereforeGUI
        self.ui = WhereforeGUI.Ui_MainWindow()
        self.ui.setupUi(self)

        self.ui.startAtSelector.addItems(["beginning", "end", "offset", "timestamp"])
        self.ui.startAtSelector.setCurrentIndex(1)
        self.ui.endAtSelector.addItems(["never", "end", "offset", "timestamp"])
        self.ui.startTimeEdit.hide()
        self.ui.startOffsetEdit.hide()
        self.ui.endOffsetEdit.hide()
        self.ui.endTimeEdit.hide()
        self.ui.startAtSelector.currentIndexChanged.connect(self.on_change_start_at)
        self.ui.endAtSelector.currentIndexChanged.connect(self.on_change_end_at)
        self.ui.brokerLed = Led(self)
        self.ui.consumerBarLayout.insertWidget(2, self.ui.brokerLed)
        self.ui.brokerAddressEdit.textEdited.connect(self.startTextEditedTimer)
        self.ui.topicPartitionSourceTree.setModel(self.topicPartitionModel)

        self.ui.topicPartitionSourceTree.selectionModel().selectionChanged.connect(self.on_tree_node_selection)

        self.ui.brokerAddressEdit.setText(self.config.value("kafka_address", type=str))
        if len(self.ui.brokerAddressEdit.text()) > 0:
            self.onBrokerEditTimer()
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
        else:
            self.selectedPartition = None
            self.selectedSource = None

    def onUpdateSelectedData(self):
        if self.selectedPartition is not None:
            partition_info = self.selectedPartition.get_partition_info()
            if partition_info is None:
                return
            self.ui.lowOffsetValue.setText(str(partition_info.low))
            self.ui.highOffsetValue.setText(str(partition_info.high))
            self.ui.lagValue.setText(str(partition_info.high - partition_info.lag))
        else:
            self.ui.lowOffsetValue.setText("n/a")
            self.ui.highOffsetValue.setText("n/a")
            self.ui.lagValue.setText("n/a")
        if self.selectedSource is not None:
            pass

    def startTextEditedTimer(self):
        self.brokerEditTimer.start(500)

    def onBrokerEditTimer(self):
        self.topicPartitionModel.set_kafka_broker(self.ui.brokerAddressEdit.text())
        self.topicUpdateFuture = self.thread_pool.submit(get_topic_partitions, self.ui.brokerAddressEdit.text())

    def onCheckForNewSources(self):
        self.topicPartitionModel.check_for_sources()

    def onCheckIfTopicsUpdated(self):
        if self.topicUpdateFuture is not None and self.topicUpdateFuture.done():
            try:
                result = self.topicUpdateFuture.result()
                if result is None:
                    self.ui.brokerLed.turn_off()
                else:
                    self.ui.brokerLed.turn_on()
                    self.updateTopicTree(result)
            except ValueError:
                pass  #Ignore

    def updateTopicTree(self, known_topics):
        self.topicPartitionModel.update_topics(known_topics)

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        self.config.setValue("kafka_address", self.ui.brokerAddressEdit.text())
        event.accept()


if __name__ == "__main__":
    # Recompile ui
    ui_file_path = Path("WhereforeGUI.ui")
    if ui_file_path.exists():
        with open(str(ui_file_path)) as ui_file:
            with open("WhereforeGUI.py", "w") as py_ui_file:
                PyQt5.uic.compileUi(ui_file, py_ui_file)

    # selector_path = Path("Data_source.ui")
    # if selector_path.exists():
    #     with open(str(selector_path)) as ui_file:
    #         with open("Data_source.py", "w") as py_ui_file:
    #             PyQt5.uic.compileUi(ui_file, py_ui_file)

    app = QtWidgets.QApplication([])
    main_window = AdcViewerApp()
    main_window.setup()
    app.exec_()
