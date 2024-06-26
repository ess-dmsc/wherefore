import configargparse as argparse
import logging
import sys
from wherefore.KafkaMessageTracker import KafkaMessageTracker, PartitionOffset
from wherefore.KafkaSecurityConfig import get_kafka_security_config
from wherefore.KafkaTopicPartitions import get_topic_partitions
from wherefore.Message import TYPE_EXTRACTOR_MAP
import time
from curses_renderer import CursesRenderer
import re
from datetime import datetime, timezone
from typing import Tuple, Optional, Union, List, Dict


def extract_point(point: str) -> Tuple[Union[datetime, int, str], Optional[int]]:
    match = re.match(r"(?P<timestamp>\d+)s(?P<offset>[+-]\d+)?$", point)
    if match is not None:
        offset_int = None
        if match["offset"] is not None:
            offset_int = int(match["offset"])
        return (
            datetime.fromtimestamp(int(match["timestamp"]), tz=timezone.utc),
            offset_int,
        )
    match = re.match(
        r"(?P<offset_str>(?:never)|(?:end)|(?:beginning))(?P<offset>[+-]\d+)?$", point
    )
    if match is not None:
        offset_int = None
        if match["offset"] is not None:
            offset_int = int(match["offset"])
        return {
            "end": PartitionOffset.END,
            "beginning": PartitionOffset.BEGINNING,
            "never": PartitionOffset.NEVER,
        }[match["offset_str"]], offset_int
    match = re.match(r"(?P<offset_int>\d+)(?P<offset_to_offset>[+-]\d+)?$", point)
    if match is not None:
        return_int = int(match["offset_int"])
        if match["offset_to_offset"] is not None:
            return_int += int(match["offset_to_offset"])
        return return_int, None
    match = re.match(r"^(?P<datetime>.+?)(?P<offset>[+-]\d+)?$", point)
    try:
        offset_int = None
        if match["offset"] is not None:
            offset_int = int(match["offset"])
        time_point = datetime.fromisoformat(match["datetime"])
        if time_point.tzinfo is None:
            time_point = time_point.replace(tzinfo=timezone.utc)
        return time_point, offset_int
    except ValueError:
        raise RuntimeError(
            f"Unable to convert timestamp, offset, or offset string from the argument: {point}"
        )


def extract_start(start: str):
    start_point = extract_point(start)
    if type(start_point[0]) is PartitionOffset and start_point == PartitionOffset.NEVER:
        raise RuntimeError('"never" is not a valid start offset.')
    return start_point


def extract_end(end: str):
    end_point = extract_point(end)
    if type(end_point[0]) is PartitionOffset and end_point == PartitionOffset.BEGINNING:
        raise RuntimeError('"beginning" is not a valid stop/end offset.')
    return end_point[0]


def print_topics(broker: str, security_config: Dict[str, str]):
    topic_partitions = get_topic_partitions(broker, security_config)
    topics = [value["name"] for value in topic_partitions]
    topics.sort()
    for t in topics:
        print(t)


def main(
    broker: str,
    topic: str,
    partition: int,
    start: str,
    end: str,
    security_config: Dict[str, str],
    schemas: Optional[List[str]],
    source_name: Optional[str],
):
    try:
        tracker = KafkaMessageTracker(
            broker,
            topic,
            partition=partition,
            start=extract_start(start),
            stop=extract_end(end),
            security_config=security_config,
            schemas=schemas,
            source_name=source_name,
        )
    except RuntimeError as e:
        print(f"Unable to enter run loop due to: {e}")
        sys.exit(1)
    renderer = CursesRenderer(topic, partition)
    while True:
        try:
            latest_update = tracker.get_latest_values()
            if latest_update is not None and len(latest_update) > 0:
                current_offsets = tracker.get_current_edge_offsets()
                max_current_offset = 0
                for source in latest_update.values():
                    msg_offset = source.last_message.offset
                    if msg_offset > max_current_offset:
                        max_current_offset = msg_offset
                current_offsets.lag = current_offsets.high - max_current_offset - 1
                if latest_update is not None:
                    renderer.set_data(latest_update)
                    renderer.set_partition_offsets(current_offsets)
            renderer.draw()
            time.sleep(0.01)
        except Exception as e:
            logging.exception(f"Error in processing loop: {e}")
        except KeyboardInterrupt:
            sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    required_args = parser.add_argument_group("required arguments")
    mutex_args = parser.add_mutually_exclusive_group(required=True)
    kafka_sec_args = parser.add_argument_group("Kafka security arguments")

    required_args.add_argument(
        "-b", "--broker", type=str, help="Address of the kafka broker.", required=True
    )

    mutex_args.add_argument("-t", "--topic", type=str, help="Topic name to listen to.")

    mutex_args.add_argument(
        "-l",
        "--list",
        action="store_true",
        help="List the topics on the current Kafka cluster and exit. Does not work with the `-t`, `-s`, `-p` and `-e` arguments.",
    )

    parser.add_argument(
        "-e",
        "--end",
        type=str,
        help='Where should consumption stop/end? Takes a datetime (e.g. `-s "2012-01-01 12:30:12"`), timestamp (e.g. `-s 1611167278s`), offset (e.g. `-s 1548647`) or one of the following strings: `end`, `never`.',
        default="never",
    )

    parser.add_argument(
        "--log",
        type=str,
        help="File name to write log messages to. Logging is disabled by default.",
    )

    parser.add_argument(
        "-p", "--partition", type=int, help="Partition to connect to.", default=0
    )

    parser.add_argument(
        "-s",
        "--start",
        type=str,
        help='Where should consumption start? Takes a datetime (e.g. `-s "2012-01-01 12:30:12"`), timestamp (e.g. `-s 1611167278s`), offset (e.g. `-s 1548647`) or one of the following strings: `beginning`, `end`. Each one of these can have an integer modifier at the end which offsets the start location. E.g. `-s end-10` or `-s "2012-01-01 12:30:12+500"`',
        default="end",
    )

    parser.add_argument(
        "-S",
        "--schemas",
        type=str,
        nargs="*",
        choices=sorted(TYPE_EXTRACTOR_MAP.keys()),
        default=None,
        help="Space-separated list of schemas. Only messages with these schemas will be shown.",
    )

    parser.add_argument(
        "--source-name",
        type=str,
        help="Monitor Kafka messages with this source_name",
    )

    kafka_sec_args.add_argument(
        "-kc",
        "--kafka-config-file",
        is_config_file=True,
        help="Kafka security configuration file",
    )

    kafka_sec_args.add_argument(
        "--security-protocol",
        type=str,
        help="Kafka security protocol",
    )

    kafka_sec_args.add_argument(
        "--sasl-mechanism",
        type=str,
        help="Kafka SASL mechanism",
    )

    kafka_sec_args.add_argument(
        "--sasl-username",
        type=str,
        help="Kafka SASL username",
    )

    kafka_sec_args.add_argument(
        "--sasl-password",
        type=str,
        help="Kafka SASL password",
    )

    kafka_sec_args.add_argument(
        "--ssl-cafile",
        type=str,
        help="Kafka SSL CA certificate path",
    )

    args = parser.parse_args()

    if args.log:
        logging.basicConfig(filename=args.log, level=logging.INFO)
    else:
        logging.disable(logging.CRITICAL)

    kafka_security_config = get_kafka_security_config(
        args.security_protocol,
        args.sasl_mechanism,
        args.sasl_username,
        args.sasl_password,
        args.ssl_cafile,
    )

    if args.list:
        print_topics(args.broker, kafka_security_config)
        exit(0)

    main(
        args.broker,
        args.topic,
        args.partition,
        args.start,
        args.end,
        kafka_security_config,
        args.schemas,
        args.source_name,
    )
