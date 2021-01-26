# wherefore

Simple tool for listing known flatbuffer sources on a Kafka topic.


## Usage

```
python wherefore.py -h
usage: wherefore.py [-h] -b BROKER -t TOPIC [-p PARTITION] [-s START] [-e END]

optional arguments:
  -h, --help            show this help message and exit
  -p PARTITION, --partition PARTITION
                        Partition to connect to.
  -s START, --start START
                        Where should consumption start? Takes a datetime (e.g.
                        `-s 2012-01-01 12:30:12`), timestamp (e.g. `-s
                        1611167278s`), offset (e.g. `-s 1548647`) or one of
                        the following strings: `beginning`, `end`.
  -e END, --end END     Where should consumption stop/end? Takes a datetime
                        (e.g. `-s 2012-01-01 12:30:12`), timestamp (e.g. `-s
                        1611167278s`), offset (e.g. `-s 1548647`) or one of
                        the following strings: `end`, `never`.

required arguments:
  -b BROKER, --broker BROKER
                        Address of the kafka broker.
  -t TOPIC, --topic TOPIC
                        Topic name to listen to.
```

The default partition if none is selected is _0_. It is possible to use different types of start and stop criteria. E.g.:
```
python wherefore.py -b some_broker -t some_topic --start beginning --end 2019-11-08 02:01
```
or
```
python wherefore.py -b some_broker -t some_topic --start 1234 --end 1611167278s
```

## Known flatbuffer schemas

As of this commit, *wherefore* is aware of the following flatbuffer schemas (from [streaming-data-types](https://github.com/ess-dmsc/streaming-data-types)):

* ev42
* hs00
* f142
* ns10
* pl72
* 624t
* x5f2
* ep00
* tdct
* rf5k
* answ
* wrdn
* ndar
* rf5k
* json
* mo01
 
 Any other schema or non-flatbuffer message will be listed as "Unknown".
 