import logging
import string

from streaming_data_types.utils import get_schema as get_schema
from streaming_data_types import (
    deserialise_6s4t,
    deserialise_ADAr,
    deserialise_al00,
    deserialise_answ,
    deserialise_ep00,
    deserialise_ep01,
    deserialise_ev42,
    deserialise_ev44,
    deserialise_f142,
    deserialise_f144,
    deserialise_hs00,
    deserialise_hs01,
    deserialise_ndar,
    deserialise_ns10,
    deserialise_pl72,
    deserialise_rf5k,
    deserialise_se00,
    deserialise_senv,
    deserialise_tdct,
    deserialise_wrdn,
    deserialise_x5f2,
    deserialise_da00,
)
from wherefore.MonitorMessage import MonitorMessage
from datetime import datetime, timezone
from typing import Tuple, Optional
import hashlib
import numpy as np
from streaming_data_types.fbschemas.epics_connection_info_ep00.EventType import (
    EventType,
)

# from streaming_data_types.epics_connection_ep01 import ConnectionInfo
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


def ev44_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_ev44(data)
    return (
        extracted.source_name,
        datetime.fromtimestamp(extracted.reference_time[0] / 1e9, tz=timezone.utc),
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


def hs01_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_hs01(data)
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
    if isinstance(extracted.value, np.ndarray) and len(extracted.value.shape) > 0:
        value_string = f"Nr of elements: {len(extracted.value)}"
    else:
        value_string = f"Value: {extracted.value}"
    return (
        extracted.source_name,
        datetime.fromtimestamp(extracted.timestamp_unix_ns / 1e9, tz=timezone.utc),
        value_string,
    )


def f144_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_f144(data)
    if isinstance(extracted.value, np.ndarray) and len(extracted.value.shape) > 0:
        value_string = f"Nr of elements: {len(extracted.value)}"
    else:
        value_string = f"Value: {extracted.value}"
    return (
        extracted.source_name,
        datetime.fromtimestamp(extracted.timestamp_unix_ns / 1e9, tz=timezone.utc),
        value_string,
    )


def al00_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_al00(data)
    value_string = f"Sev: {extracted.severity.name}, Msg: {extracted.message}"
    return (
        extracted.source,
        datetime.fromtimestamp(extracted.timestamp_ns / 1e9, tz=timezone.utc),
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


def ep01_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    extracted = deserialise_ep01(data)
    return (
        extracted.source_name,
        datetime.fromtimestamp(extracted.timestamp / 1e9, tz=timezone.utc),
        f"{extracted.source_name} status: {extracted.status.name.capitalize()}",
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


def se00_extractor(data: bytes) -> Tuple[str, Optional[datetime], str]:
    message = deserialise_se00(data)
    return (
        message.name,
        datetime.fromtimestamp(message.timestamp_unix_ns / 1e9, tz=timezone.utc),
        f"Nr of elements: {len(message.values)}",
    )

def da00_extractor(data: bytes) -> Tuple[str, datetime | None, str]:
    message = deserialise_da00(data)
    return (
        message.source_name,
        datetime.fromtimestamp(message.timestamp_ns / 1e9, tz=timezone.utc),
        f"{len(message.data)} Variables: {','.join(x.name for x in message.data)}",
    )


TYPE_EXTRACTOR_MAP = {
    "6s4t": s_6s4t_extractor,
    "ADAr": adar_extractor,
    "answ": answ_extractor,
    "ep00": ep00_extractor,
    "ep01": ep01_extractor,
    "ev42": ev42_extractor,
    "f142": f142_extractor,
    "hs00": hs00_extractor,
    "json": json_extractor,
    "mo01": mo01_extractor,
    "NDAr": ndar_extractor,
    "ns10": ns10_extractor,
    "pl72": pl72_extractor,
    "rf5k": rf5k_extractor,
    "senv": senv_extractor,
    "tdct": tdct_extractor,
    "wrdn": wrdn_extractor,
    "x5f2": x5f2_extractor,
    "ev44": ev44_extractor,
    "hs01": hs01_extractor,
    "f144": f144_extractor,
    "se00": se00_extractor,
    "al00": al00_extractor,
    "da00": da00_extractor,
}


def unknown_extractor(name: str):
    def named_extractor(data: bytes) -> Tuple[str, datetime | None, str]:
        return f"Unknown {name} source", None, f"No extractor for {name} (yet)"

    return named_extractor


def extract_message_info(message_data: bytes) -> Tuple[str, str, datetime, str]:
    message_type = get_schema(
        message_data
    )  # TODO add error handling for messages too short
    extractor = TYPE_EXTRACTOR_MAP.get(message_type, unknown_extractor(message_type))
    try:
        source, timestamp, display_message = extractor(message_data)
        source = "".join(letter for letter in source if letter in string.printable)
    except Exception as exc:
        logging.exception(f"Error decoding schema {message_type}: {exc}")
        return message_type, "Unknown", None, exc
    return message_type, source, timestamp, display_message


class Message:
    def __init__(self, value, timestamp_s, offset):
        self._local_time = datetime.now(tz=timezone.utc)
        (
            self._message_type,
            self._source_name,
            self._message_time,
            self._data,
        ) = extract_message_info(value)
        hash_generator = hashlib.sha256()
        hash_generator.update(self._source_name.encode())
        hash_generator.update(self._message_type.encode())
        self._source_hash = hash_generator.digest()
        self._kafka_time = datetime.fromtimestamp(timestamp_s, tz=timezone.utc)
        self._value = value
        self._offset = offset
        self._size = len(value)

    def __repr__(self):
        return f"{self._message_type}/{self._source_name}/{self._message_time}/{self._kafka_time}/{self._data}"

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
