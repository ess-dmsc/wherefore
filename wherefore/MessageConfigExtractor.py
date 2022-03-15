from typing import Dict, Callable, Any
from streaming_data_types.utils import get_schema as get_schema
import numpy as np
from streaming_data_types import (
    deserialise_ADAr,
    deserialise_f142,
    deserialise_senv,
)


def get_f142_config(msg: bytes) -> Dict[str, Any]:
    numpy_type_to_string = {
        np.dtype("byte"): "int8",
        np.dtype("ubyte"): "uint8",
        np.dtype("int8"): "int8",
        np.dtype("int16"): "int16",
        np.dtype("int32"): "int32",
        np.dtype("int64"): "int64",
        np.dtype("uint8"): "uint8",
        np.dtype("uint16"): "uint16",
        np.dtype("uint32"): "uint32",
        np.dtype("uint64"): "uint64",
        np.dtype("float32"): "float32",
        np.dtype("float64"): "float64",
    }
    result = deserialise_f142(msg)
    return_dict = {"type": numpy_type_to_string[result.value.dtype], }
    if len(result.value.shape) == 1:
        return_dict["array_size"] = result.value.shape[0]
    return return_dict


def get_senv_config(msg: bytes) -> Dict[str, Any]:
    numpy_type_to_string = {
        np.dtype("byte"): "int8",
        np.dtype("ubyte"): "uint8",
        np.dtype("int8"): "int8",
        np.dtype("int16"): "int16",
        np.dtype("int32"): "int32",
        np.dtype("int64"): "int64",
        np.dtype("uint8"): "uint8",
        np.dtype("uint16"): "uint16",
        np.dtype("uint32"): "uint32",
        np.dtype("uint64"): "uint64",
    }
    result = deserialise_senv(msg)
    return {"type": numpy_type_to_string[result.values.dtype], }


def get_ADAr_config(msg: bytes) -> Dict[str, Any]:
    numpy_type_to_string = {
        np.dtype("byte"): "int8",
        np.dtype("ubyte"): "uint8",
        np.dtype("int8"): "int8",
        np.dtype("int16"): "int16",
        np.dtype("int32"): "int32",
        np.dtype("int64"): "int64",
        np.dtype("uint8"): "uint8",
        np.dtype("uint16"): "uint16",
        np.dtype("uint32"): "uint32",
        np.dtype("uint64"): "uint64",
        np.dtype("float32"): "float32",
        np.dtype("float64"): "float64",
    }
    result = deserialise_ADAr(msg)
    return {"type": numpy_type_to_string[result.data.dtype], "array_size": result.data.shape}


def get_extra_config(msg: bytes) -> Dict:
    config_extractor_map: Dict[str, Callable[[bytes], Dict[str, str]]] = {
        "f142": get_f142_config,
        "senv": get_senv_config,
        "ADAr": get_ADAr_config,
    }
    message_type = get_schema(msg)
    try:
        extractor = config_extractor_map[message_type]
    except KeyError:
        return {}
    return extractor(msg)
