from streaming_data_types.utils import get_schema as get_schema
from streaming_data_types import (
    deserialise_ev42,
    deserialise_hs00,
    deserialise_wrdn,
    deserialise_f142,
    deserialise_ns10,
    deserialise_pl72,
    deserialise_6s4t,
    deserialise_x5f2,
    deserialise_ep00,
    deserialise_tdct,
    deserialise_rf5k,
    deserialise_answ,
    deserialise_ndar,
    deserialise_ADAr,
    deserialise_senv,
)
from wherefore.MonitorMessage import MonitorMessage
from datetime import datetime, timezone
from typing import Tuple, Optional
import hashlib
import numpy as np
from streaming_data_types.fbschemas.epics_connection_info_ep00.EventType import (
    EventType,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)
from streaming_data_types.fbschemas.action_response_answ.ActionOutcome import (
    ActionOutcome,
)
from streaming_data_types.fbschemas.action_response_answ.ActionType import ActionType


def ev42_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_ev42(data)
    return (
        extracted.source_name,
        datetime.fromtimestamp(extracted.pulse_time / 1e9, tz=timezone.utc),
        f"Nr of events: {len(extracted.time_of_flight)}",
    )


def hs00_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_hs00(data)
    dims_string = extracted["dim_metadata"][0]["length"]
    for i in range(1, len(extracted["dim_metadata"])):
        dims_string += "x" + str(extracted["dim_metadata"][i]["length"])
    return (
        extracted["source"],
        datetime.fromtimestamp(extracted["timestamp"] / 1e9, tz=timezone.utc),
        f"Histogram dimensions: {dims_string}",
    )


def f142_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_f142(data)
    if len(extracted.value.shape) == 0:
        value_string = f"Value: {extracted.value}"
    else:
        value_string = f"Nr of elements: {len(extracted.value)}"
    return (
        extracted.source_name,
        datetime.fromtimestamp(extracted.timestamp_unix_ns / 1e9, tz=timezone.utc),
        value_string,
    )


def ns10_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_ns10(data)
    return (
        extracted.key,
        datetime.fromtimestamp(extracted.time_stamp, tz=timezone.utc),
        f'Key: "{extracted.key}", Value: "{extracted.value}"',
    )


def pl72_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_pl72(data)
    return (
        extracted.service_id,
        None,
        "Start time: "
        + datetime.fromtimestamp(extracted.start_time / 1e3).strftime("%Y-%m-%d %H:%M"),
    )


def s_6s4t_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_6s4t(data)
    return (
        extracted.service_id,
        None,
        "Stop time: "
        + datetime.fromtimestamp(extracted.stop_time / 1e3).strftime("%Y-%m-%d %H:%M"),
    )


def x5f2_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_x5f2(data)
    return (
        extracted.service_id,
        None,
        f"{extracted.service_id} status ({extracted.software_name} on {extracted.host_name})",
    )


def ep00_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_ep00(data)
    type_map = {
        EventType.UNKNOWN: "Unknown",
        EventType.CONNECTED: "Connected",
        EventType.DISCONNECTED: "Disconnected",
        EventType.DESTROYED: "Destroyed",
        EventType.NEVER_CONNECTED: "Never connected",
    }
    return (
        extracted.source_name,
        datetime.fromtimestamp(extracted.timestamp / 1e9, tz=timezone.utc),
        f"{extracted.source_name} status: {type_map[extracted.type]}",
    )


def tdct_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_tdct(data)
    return (
        extracted.name,
        datetime.fromtimestamp(extracted.timestamps[0] / 1e9, tz=timezone.utc),
        f"Nr of timestamps: {len(extracted.timestamps)}",
    )


def rf5k_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_rf5k(data)
    type_map = {UpdateType.ADD: "Addition", UpdateType.REMOVE: "Removal"}
    if extracted.config_change == UpdateType.REMOVEALL:
        data_string = "Remove all streams"
    else:
        data_string = (
            f"{type_map[extracted.config_change]} of {len(extracted.streams)} streams"
        )
    return "forwarder_config", None, data_string


def answ_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_answ(data)
    type_map = {
        ActionType.StartJob: "Start job",
        ActionType.SetStopTime: "Set stop time",
    }
    result_map = {
        ActionOutcome.Failure: "failed",
        ActionOutcome.Success: "was successfull!",
    }
    return (
        extracted.service_id,
        None,
        f"{type_map[extracted.action]} command {result_map[extracted.outcome]}",
    )


def wrdn_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_wrdn(data)
    return (
        extracted.service_id,
        None,
        f'Writing of file with name "{extracted.file_name}" finished',
    )


def ndar_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_ndar(data)
    dims_str = str(extracted.data.shape[0])
    for i in range(1, len(extracted.data.shape)):
        dims_str += "x" + str(extracted.data.shape[i])
    return (
        "AreaDetector_data",
        datetime.fromtimestamp(extracted.timestamp + 631152000, tz=timezone.utc),
        f"Dimensions: {dims_str}",
    )


def adar_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_ADAr(data)
    dims_str = str(extracted.data.shape[0])
    for i in range(1, len(extracted.data.shape)):
        dims_str += "x" + str(extracted.data.shape[i])
    return extracted.source_name, extracted.timestamp, f"Dimensions: {dims_str}"


def json_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    decoded_str = data.decode("utf-8")
    if len(decoded_str) < 15:
        used_string = decoded_str
    else:
        used_string = decoded_str[:15] + "..."
    return "JSON in Flatbuffer", None, used_string


def mo01_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    message = MonitorMessage.GetRootAsMonitorMessage(data, 0)
    return message.SourceName().decode(), None, "No data extracted"


def senv_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    message = deserialise_senv(data)
    return message.name, message.timestamp, f"Nr of elements: {len(message.values)}"


def extract_message_info(message_data: bytes) -> Tuple[str, str, datetime, str]:
    message_type = get_schema(message_data)
    type_extractor_map = {
        "ev42": ev42_extractor,
        "hs00": hs00_extractor,
        "f142": f142_extractor,
        "ns10": ns10_extractor,
        "pl72": pl72_extractor,
        "6s4t": s_6s4t_extractor,
        "x5f2": x5f2_extractor,
        "ep00": ep00_extractor,
        "tdct": tdct_extractor,
        "rf5k": rf5k_extractor,
        "answ": answ_extractor,
        "wrdn": wrdn_extractor,
        "NDAr": ndar_extractor,
        "ADAr": adar_extractor,
        "json": json_extractor,
        "mo01": mo01_extractor,
        "senv": senv_extractor,
    }
    try:
        extractor = type_extractor_map[message_type]
    except KeyError:
        return "Unknown", "Unknown", None, "No data"
    return message_type, *extractor(message_data)


class Message:
    def __init__(self, kafka_msg):
        self._local_time = datetime.now(tz=timezone.utc)
        (
            self._message_type,
            self._source_name,
            self._message_time,
            self._data,
        ) = extract_message_info(kafka_msg.value)
        hash_generator = hashlib.sha256()
        hash_generator.update(self._source_name.encode())
        hash_generator.update(self._message_type.encode())
        self._source_hash = hash_generator.digest()
        self._kafka_time = datetime.fromtimestamp(
            kafka_msg.timestamp / 1e3, tz=timezone.utc
        )
        self._value = kafka_msg.value
        self._offset = kafka_msg.offset
        self._size = len(kafka_msg.value)

    @property
    def source_hash(self) -> bytes:
        return self._source_hash

    @property
    def source_name(self) -> str:
        return self._source_name

    @property
    def message_type(self) -> str:
        return self._message_type

    @property
    def local_timestamp(self) -> datetime:
        return self._local_time

    @property
    def timestamp(self) -> datetime:
        return self._message_time

    @property
    def kafka_timestamp(self) -> datetime:
        return self._kafka_time

    @property
    def offset(self):
        return self._offset

    @property
    def size(self):
        return self._size

    @property
    def data(self):
        return self._data
