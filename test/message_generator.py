from streaming_data_types import serialise_f142
from kafka import KafkaProducer
import argparse
import time
import randomname


def main():
    parser = argparse.ArgumentParser()

    required_args = parser.add_argument_group("required arguments")
    required_args.add_argument(
        "-b", "--broker", type=str, help="Address of the kafka broker.", required=True
    )

    required_args.add_argument(
        "-t", "--topic", type=str, help="Topic name to listen to.", required=True
    )
    parser.add_argument("-n", type=int, help="Number of sources to produce messages for.", default=1)
    args = parser.parse_args()
    used_names = [randomname.get_name() for i in range(args.n)]
    producer = KafkaProducer(bootstrap_servers=args.broker)

    while True:
        for i in range(args.n):
            producer.send(topic=args.topic, value=serialise_f142(1, used_names[i], int(time.time()*1e9)))
        time.sleep(1)


if __name__ == "__main__":
    main()
