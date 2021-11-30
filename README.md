# shredder
shredds Fixed column file to avro/kafka .  
Implementation uses Avro schema and multicore   
Speed around 220mb/sec per Core using 4 core on a 1Gb/s kafka connection

Notes current features/limitations:
* You kafka partition amount must be equal to core's used  
* Multicore implementation.
* Each go routine sends to corresponding partition. ie. 8 cores -> 8 go routiens -> 8 partitions
* Fixed/supported input format is utf8  and utf8 output (iso8859-1 etc will be supported)

# syntax
```console
shredder.exe <kafka broker> <chemaregistry> <schema file url> <schema id> <topic> <cores=partitions> <data file>
```

# Performance example 1 using 12 cores ( output snappy avro files 30 columns)
Hardware: 12 core (Amd Threadripper 5960X),1Gb kafka connection  , Samsung 980 pro 7/5 Gb r/w sec.  
Datafile: 1.3Gb , 30 columns, total 528 chars (runes)  row width.

```console
rickard@Oden-Threadripper:~/GolandProjects/shredder2$ ./shredder /tmp/avrofiles 10.1.1.90:8081 schema1.json 2 table_x14 12 test.last111
Schema = {
"type": "record",
"name": "weblog",
"fields" : [
... <30 columns removed from readme >
Time spend in total     : 1.845484353s  parsing  4960143  lines from  2620609413  bytes
Troughput bytes/s total : 1.32GB /s
Troughput lines/s total : 2.56M  Lines/s
Troughput lines/s toAvro: 3.28M  Lines/s
Time spent toReadChunks : 0.0282819985 s
Time spent toAvro       : 1.4421747465 s
Time spent WaitDoneExport      : 0.042694734 s
```

# Performance example 2 using 6 cores ( output avro to kafka 30 columns)
Hardware: 6 core (Amd Threadripper 5960X),1Gb kafka connection  , Samsung 980 pro 7/5 Gb r/w sec.  
Datafile: 1.3Gb , 30 columns, total 528 chars (runes)  row width.
```console

rickard@Oden-Threadripper:~/GolandProjects/shredder2$ ./shredder 10.1.1.90:9092 10.1.1.90:8081 schema1.json 2 table_x14 8 test.last111
Schema = {
"type": "record",
"name": "weblog",
"fields" : [
... <30 columns removed from readme >
skipping footer
Time spend in total     : 1.713205915s  parsing  2590562  lines from  1367816800  bytes
Troughput bytes/s total : 761.41MB /s
Troughput lines/s total : 1.44M  Lines/s
Troughput lines/s toAvro: 2.82M  Lines/s
Time spent toReadChunks : 0.0215806745 s
Time spent toAvro       : 0.87478208175 s
Time spent toKafka      : 0.59487903675 s
```

# Performance example 3 using 48 cores ( output avro to kafka 30 columns)
Hardware: 48 core (Amd Threadripper 5960X),1Gb kafka connection  , Samsung 980 pro 7/5 Gb r/w sec.  
Datafile: 1.3Gb , 30 columns, total 528 chars (runes)  row width.
```console

rickard@Oden-Threadripper:~/GolandProjects/shredder2$ ./shredder /tmp/avrofiles 10.1.1.90:8081 schema1.json 2 table_x14 48 test.last111
Schema = {
"type": "record",
"name": "weblog",``
"fields" : [
... <30 columns removed from readme >
skipping footer
Time spend in total     : 954.359385ms  parsing  4960143  lines from  2620609413  bytes
Troughput bytes/s total : 2.56GB /s
Troughput lines/s total : 4.96M  Lines/s
Troughput lines/s toAvro: 9.03M  Lines/s
Time spent toReadChunks : 0.0084119796875 s
Time spent toAvro       : 0.5240676388958333 s
Time spent WaitDoneExport      : 0.012015946 s```
NOTE: Time spent ToKafka is the the transfer time from "Shredder" to librd the underlying the kafka client library)

# Example schema
Note that column name needs a capital first character.
```console

{
"type": "record",
"name": "weblog",
"fields" : [
    {"name": "Idnr", "type":{"type": "long","name": "Idnr", "len":8}},
    {"name": "Event_time", "type":{"type" : "long", "logicalType" : "timestamp-micros","name":"Event_time", "len":26}},
    {"name": "Idnr2", "type":{"type": "int","name": "Idnr2", "len":6}},
    {"name": "Ok", "type":{"type": "boolean","name": "Ok", "len":1}},
    {"name": "Some_text1", "type":{"type": "string","name": "Some_text1", "len":30}},
    {"name": "Some_text2", "type":{"type": "string","name": "Some_text2", "len":30}},
   ]
}
```

# Credits
* Included kafka/avro client code origins from https://github.com/mycujoo/go-kafka-avro from mycujoo.tv "Democratizing football broadcasting."  
* Imported go module hamba/avro gives excellent speed and their team have been helpful on upcoming optimizations  https://github.com/hamba/avro  

# Future
* Improve speed by by taking inspiration from this https://teivah.medium.com/go-and-cpu-caches-af5d32cc5592
* Further speed improvements possible from a slight correction of Shredders usage of hamba/avro 
* Once Go "port" of apache arrow / parquet is done (jira ARROW-7905) ,merge in apache arrow based shredder , that adds Parquet as output.
