# wherefore

Simple tool for listing known flatbuffer sources on a Kafka topic. **Note: the GUI version is currently not working and needs to be updated after the migration to confluent-kafka.**


## Usage

```
python wherefore.py -h
usage: wherefore.py [-h] -b BROKER (-t TOPIC | -l) [-e END] [--log LOG] [-p PARTITION] [-s START]
                    [-S [{6s4t,ADAr,NDAr,al00,answ,ep00,ep01,ev42,ev44,f142,f144,hs00,hs01,json,mo01,ns10,pl72,rf5k,se00,senv,tdct,wrdn,x5f2} ...]]

options:
  -h, --help            show this help message and exit
  -t TOPIC, --topic TOPIC
                        Topic name to listen to.
  -l, --list            List the topics on the current Kafka cluster and exit. Does not work with the `-t`, `-s`, `-p` and `-e` arguments.
  -e END, --end END     Where should consumption stop/end? Takes a datetime (e.g. `-s "2012-01-01 12:30:12"`), timestamp (e.g. `-s 1611167278s`), offset
                        (e.g. `-s 1548647`) or one of the following strings: `end`, `never`.
  --log LOG             File name to write log messages to. Logging is disabled by default.
  -p PARTITION, --partition PARTITION
                        Partition to connect to.
  -s START, --start START
                        Where should consumption start? Takes a datetime (e.g. `-s "2012-01-01 12:30:12"`), timestamp (e.g. `-s 1611167278s`), offset
                        (e.g. `-s 1548647`) or one of the following strings: `beginning`, `end`. Each one of these can have an integer modifier at the
                        end which offsets the start location. E.g. `-s end-10` or `-s "2012-01-01 12:30:12+500"`
  -S [{6s4t,ADAr,NDAr,al00,answ,ep00,ep01,ev42,ev44,f142,f144,hs00,hs01,json,mo01,ns10,pl72,rf5k,se00,senv,tdct,wrdn,x5f2} ...], --schemas [{6s4t,ADAr,NDAr,al00,answ,ep00,ep01,ev42,ev44,f142,f144,hs00,hs01,json,mo01,ns10,pl72,rf5k,se00,senv,tdct,wrdn,x5f2} ...]
                        Space-separated list of schemas. Only messages with these schemas will be shown.

required arguments:
  -b BROKER, --broker BROKER
                        Address of the kafka broker.
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

* 624t
* al00
* answ
* ep00
* ep01
* ev42
* ev44
* f142
* f144
* hs00
* hs01
* json
* mo01
* ndar
* ns10
* pl72
* rf5k
* rf5k
* se00
* tdct
* wrdn
* x5f2

 
 Any other schema or non-flatbuffer message will be listed as "Unknown".
 
