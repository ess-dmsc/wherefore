import random
import argparse
import logging
import sys
from wherefore.KafkaMessageTracker import KafkaMessageTracker, PartitionOffset
from wherefore.KafkaTopicPartitions import get_topic_partitions
from wherefore.Message import TYPE_EXTRACTOR_MAP, Message
from wherefore.DataSource import DataSource
import time
import re
from datetime import datetime, timezone
from typing import Tuple, Optional, Union, List, Dict
from graphyte import Sender


GRAPHITE_ADDRESS = "10.100.211.201"


def extract_point(point: str) -> Tuple[Union[datetime, int, str], Optional[int]]:
    match = re.match("(?P<timestamp>\d+)s(?P<offset>[+-]\d+)?$", point)
    if match != None:
        offset_int = None
        if match["offset"] is not None:
            offset_int = int(match["offset"])
        return (
            datetime.fromtimestamp(int(match["timestamp"]), tz=timezone.utc),
            offset_int,
        )
    match = re.match(
        "(?P<offset_str>(?:never)|(?:end)|(?:beginning))(?P<offset>[+-]\d+)?$", point
    )
    if match != None:
        offset_int = None
        if match["offset"] is not None:
            offset_int = int(match["offset"])
        return {
            "end": PartitionOffset.END,
            "beginning": PartitionOffset.BEGINNING,
            "never": PartitionOffset.NEVER,
        }[match["offset_str"]], offset_int
    match = re.match("(?P<offset_int>\d+)(?P<offset_to_offset>[+-]\d+)?$", point)
    if match != None:
        return_int = int(match["offset_int"])
        if match["offset_to_offset"] is not None:
            return_int += int(match["offset_to_offset"])
        return return_int, None
    match = re.match("^(?P<datetime>.+?)(?P<offset>[+-]\d+)?$", point)
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


def print_topics(broker: str):
    topic_partitions = get_topic_partitions(broker)
    topics = [value["name"] for value in topic_partitions]
    topics.sort()
    for t in topics:
        print(t)


def process_message(msg: Message) -> None:
    prefix = "latency.epics_to_consumer"
    sender = Sender(GRAPHITE_ADDRESS, prefix=prefix)
    now = datetime.now(tz=timezone.utc)
    latency = (now - msg.timestamp).total_seconds()
    logging.debug(f"Pushing metric: {now.timestamp()} {prefix}.{msg.source_name} {latency}")
    sender.send(
        msg.source_name, latency, now.timestamp()
    )


def main(broker: str, topic: str, partition: int, start: str, end: str, schemas: Optional[List[str]], source_name: Optional[str]):
    try:
        tracker = KafkaMessageTracker(
            broker,
            topic,
            partition=partition,
            start=extract_start(start),
            stop=extract_end(end),
            schemas=schemas,
            source_name=source_name,
        )
    except RuntimeError as e:
        print(f"Unable to enter run loop due to: {e}")
        sys.exit(1)
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
                    if len(latest_update.keys()) != 1:
                        logging.warning("The message contained several Datasources, skipping: {}".format(
                            latest_update
                        ))
                        continue
                    datasource: DataSource = list(latest_update.values())[0]
                    if not datasource.last_message:
                        logging.warning("Latest message is empty for datasource, skipping: {}".format(
                            datasource
                        ))
                        continue
                    process_message(datasource.last_message)
                    return
            time.sleep(0.01)
        except Exception as e:
            logging.exception(f"Error in processing loop: {e}")
        except KeyboardInterrupt:
            sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    required_args = parser.add_argument_group("required arguments")
    mutex_args = parser.add_mutually_exclusive_group(required=True)
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
        help="File name to write log messages to. Logging is disabled by default."
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
        help='Space-separated list of schemas. Only messages with these schemas will be shown.',
    )

    parser.add_argument(
        "--source-name",
        type=str,
        help='Monitor Kafka messages with this source_name',
        required=True,
    )

    args = parser.parse_args()
    if args.log:
        logging.basicConfig(filename=args.log, level=logging.INFO)
    else:
        logging.disable(logging.CRITICAL)
    if args.list:
        print_topics(args.broker)
        exit(0)
    main(args.broker, args.topic, args.partition, args.start, args.end, args.schemas, args.source_name)
