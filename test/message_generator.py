from streaming_data_types import (
    serialise_f142,
    serialise_ev42,
    serialise_hs00,
    serialise_ns10,
    serialise_pl72,
    serialise_6s4t,
    serialise_x5f2,
    serialise_ep00,
    serialise_ep01,
    serialise_tdct,
    serialise_rf5k,
    serialise_answ,
    serialise_wrdn,
    serialise_ndar,
    serialise_ADAr,
    serialise_senv
)
from streaming_data_types.fbschemas.epics_connection_info_ep00.EventType import (
    EventType,
)
from streaming_data_types.epics_connection_ep01 import ConnectionInfo
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)
from streaming_data_types.fbschemas.action_response_answ.ActionType import ActionType
from streaming_data_types.fbschemas.action_response_answ.ActionOutcome import (
    ActionOutcome,
)
from kafka import KafkaProducer
import argparse
import time
import randomname
import numpy as np
from datetime import datetime
from json import dumps


def create_hs00_test_data(name, time):
    return {
        "source": name,
        "timestamp": time,
        "current_shape": [5],
        "dim_metadata": [
            {
                "length": 5,
                "unit": "m",
                "label": "some_label",
                "bin_boundaries": np.array([0, 1, 2, 3, 4, 5], dtype=np.float32),
            }
        ],
        "last_metadata_timestamp": time,
        "data": np.array([1, 2, 3, 4, 5], dtype=np.float32),
        "errors": np.array([5, 4, 3, 2, 1], dtype=np.float32),
        "info": "info_string",
    }


list_of_serialisers = [
    lambda name, time: serialise_f142(42, name, time),
    lambda name, time: serialise_f142(np.random.rand(4), name, time),
    lambda name, time: serialise_ev42(name, 1234, time, np.arange(10), np.arange(10)),
    lambda name, time: serialise_hs00(create_hs00_test_data(name, time)),
    lambda name, time: serialise_ns10(
        key=name, value="42", time_stamp=time / 1e9, ttl=111
    ),
    lambda name, time: serialise_pl72(
        "some_job_id", "some_file_name.nxs", int(time / 1e6), None, service_id=name
    ),
    lambda name, time: serialise_6s4t(
        "some_job_id",
        service_id=name,
        command_id="some_command_id",
        stop_time=datetime.now(),
    ),
    lambda name, time: serialise_x5f2(
        "some_app_name", "v1.2.3", name, "some_host_name", 123456, 65431, "{}"
    ),
    lambda name, time: serialise_ep00(time, EventType.CONNECTED, name),
    lambda name, time: serialise_ep01(time, ConnectionInfo.CONNECTED, name),
    lambda name, time: serialise_tdct(name, time + np.arange(10)),
    lambda name, time: serialise_rf5k(UpdateType.REMOVE, []),
    lambda name, time: serialise_answ(
        name,
        "some_job_id",
        "some_command_id",
        action=ActionType.StartJob,
        outcome=ActionOutcome.Failure,
        message="Some message.",
        status_code=432,
        stop_time=datetime.now(),
    ),
    lambda name, time: serialise_wrdn(name, "some_job_id", False, "some_file_name.hdf"),
    lambda name, time: serialise_ADAr(
        name, 1234, datetime.fromtimestamp(time / 1e9), np.random.randint(0, 20, [5,10,15])
    ),
    # lambda name, time: serialise_ndar(id=name, dims=[2,2], data_type=3, data=[1,2,3,4]),
    lambda name, time: dumps({"name": name, "time": time}).encode("utf-8"),
    lambda name, time: serialise_senv(name, 3, datetime.fromtimestamp(time/1e9), 10, 111, np.arange(50)),
]


def main():
    parser = argparse.ArgumentParser()

    required_args = parser.add_argument_group("required arguments")
    required_args.add_argument(
        "-b", "--broker", type=str, help="Address of the kafka broker.", required=True
    )

    required_args.add_argument(
        "-t", "--topic", type=str, help="Topic name to listen to.", required=True
    )
    parser.add_argument(
        "-n", type=int, help="Number of sources to produce messages for.", default=1
    )
    args = parser.parse_args()
    used_names = [randomname.get_name() for i in range(args.n)]
    producer = KafkaProducer(bootstrap_servers=args.broker)

    while True:
        for i in range(args.n):
            producer.send(
                topic=args.topic,
                value=list_of_serialisers[i % len(list_of_serialisers)](
                    used_names[i], int(time.time() * 1e9)
                ),
            )
        time.sleep(1)


if __name__ == "__main__":
    main()
