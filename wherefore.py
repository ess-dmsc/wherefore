import argparse
from wherefore.KafkaMessageTracker import KafkaMessageTracker, PartitionOffset
import time
from curses_renderer import CursesRenderer
import re
from datetime import datetime, timezone


def extract_point(point: str):
    match = re.match("(?P<timestamp>\d+)s$", point)
    if match != None:
        return datetime.fromtimestamp(int(match["timestamp"]), tz=timezone.utc)
    match = re.match("(?P<offset_str>(?:never)|(?:end)|(?:beginning))$", point)
    if match != None:
        return {"end": PartitionOffset.END, "beginning": PartitionOffset.BEGINNING, "never": PartitionOffset.NEVER}[match["offset_str"]]
    match = re.match("(?P<offset>\d+)$", point)
    if match != None:
        return int(match["offset"])
    try:
        time_point = datetime.fromisoformat(point)
        if time_point.tzinfo is None:
            time_point = time_point.replace(tzinfo=timezone.utc)
        return time_point
    except ValueError:
        raise RuntimeError(f"Unable to convert timestamp, offset, or offset string from the argument: {point}")


def extract_start(start: str):
    start_point = extract_point(start)
    if type(start_point) is PartitionOffset and start_point == PartitionOffset.NEVER:
        raise RuntimeError("\"never\" is not a valid start offset.")
    return start_point


def extract_end(end: str):
    end_point = extract_point(end)
    if type(end_point) is PartitionOffset and end_point == PartitionOffset.BEGINNING:
        raise RuntimeError("\"beginning\" is not a valid stop/end offset.")
    return end_point


def main(broker: str, topic: str, partition: int, start: str, end: str):
    tracker = KafkaMessageTracker(broker, topic, partition=partition, start=extract_start(start), stop=extract_end(end))
    renderer = CursesRenderer(topic, partition)
    try:
        while True:
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
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    required_args = parser.add_argument_group("required arguments")
    required_args.add_argument(
        "-b", "--broker", type=str, help="Address of the kafka broker.", required=True
    )

    required_args.add_argument(
        "-t", "--topic", type=str, help="Topic name to listen to.", required=True
    )

    parser.add_argument("-p", "--partition", type=int, help="Partition to connect to.", default=0)

    parser.add_argument(
        "-s",
        "--start",
        type=str,
        help="Where should consumption start? Takes a datetime (e.g. `-s \"2012-01-01 12:30:12\"`), timestamp (e.g. `-s 1611167278s`), offset (e.g. `-s 1548647`) or one of the following strings: `beginning`, `end`.",
        default="end"
    )

    parser.add_argument(
        "-e",
        "--end",
        type=str,
        help="Where should consumption stop/end? Takes a datetime (e.g. `-s \"2012-01-01 12:30:12\"`), timestamp (e.g. `-s 1611167278s`), offset (e.g. `-s 1548647`) or one of the following strings: `end`, `never`.",
        default="never"
    )

    args = parser.parse_args()
    main(args.broker, args.topic, args.partition, args.start, args.end)
