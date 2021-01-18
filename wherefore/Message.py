from streaming_data_types.utils import get_schema
from streaming_data_types import deserialise_ev42,deserialise_hs00, deserialise_wrdn, deserialise_f142, deserialise_ns10, deserialise_pl72, deserialise_6s4t, deserialise_x5f2, deserialise_ep00, deserialise_tdct, deserialise_rf5k, deserialise_answ, deserialise_ndar
from datetime import datetime
from typing import Tuple
import hashlib


def ev42_extractor(data: bytes):
    extracted = deserialise_ev42(data)
    return extracted.source_name, datetime.fromtimestamp(extracted.pulse_time / 1e9)


def hs00_extractor(data: bytes):
    extracted = deserialise_hs00(data)
    return extracted.source, datetime.fromtimestamp(extracted.timestamp / 1e9)


def f142_extractor(data: bytes):
    extracted = deserialise_f142(data)
    return extracted.source_name, datetime.fromtimestamp(extracted.timestamp_unix_ns / 1e9)


def ns10_extractor(data: bytes):
    extracted = deserialise_ns10(data)
    return extracted.key, datetime.fromtimestamp(extracted.time_stamp)


def pl72_extractor(data: bytes):
    extracted = deserialise_pl72(data)
    return extracted.service_id, None


def s_6s4t_extractor(data: bytes):
    extracted = deserialise_6s4t(data)
    return extracted.service_id, None


def x5f2_extractor(data: bytes):
    extracted = deserialise_x5f2(data)
    return extracted.service_id, None


def ep00_extractor(data: bytes):
    extracted = deserialise_ep00(data)
    return extracted.source_name, datetime.fromtimestamp(extracted.timestamp / 1e9)


def tdct_extractor(data: bytes):
    extracted = deserialise_tdct(data)
    return extracted.name, datetime.fromtimestamp(extracted.timestamps[0] / 1e9)


def rf5k_extractor(data: bytes):
    deserialise_rf5k(data)
    return "forwarder_config", None


def answ_extractor(data: bytes):
    extracted = deserialise_answ(data)
    return extracted.service_id, None


def wrdn_extractor(data: bytes):
    extracted = deserialise_wrdn(data)
    return extracted.service_id, None


def ndar_extractor(data: bytes):
    extracted = deserialise_ndar(data)
    return "AreaDetector_data", extracted.timestamp


def extract_message_info(message_data: bytes) -> Tuple[str, str, datetime]:
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
    }
    try:
        name, timestamp = type_extractor_map[message_type](message_data)
        return message_type, name, timestamp
    except KeyError:
        return "Unknown", "Unknown", None


class Message:
    def __init__(self, kafka_msg):
        self.local_time = datetime.now()
        self._message_type, self._source_name, self._message_time = extract_message_info(kafka_msg.value)
        hash_generator = hashlib.sha256()
        hash_generator.update(self._source_name.encode())
        hash_generator.update(self._message_type.encode())
        self._source_hash = hash_generator.digest()
        self._kafka_time = datetime.fromtimestamp(kafka_msg.timestamp / 1e3)
        self._value = kafka_msg.value

    @property
    def source_hash(self) -> bytes:
        return self._source_hash

    @property
    def source_name(self) -> str:
        return self._source_name

    @property
    def message_type(self) -> str:
        return self._message_type

