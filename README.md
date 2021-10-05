# shredder
shredds Fixed column file to avro/kafka .  
Implementation uses Avro schema and multicore   
Speed around 220mb/sec per Core. where 4 core  fills 1000meg/s bandwith to Kafka

Notes:
* Currently avro deserializing dislikes the data in kafka :) When this is fixed we have a 1.0 version :)
* Multicore implementation.
* Currently fixed/supported input format is 8859-1 and utf8 output
 
# Performance example
Hardware: 6 core (Amd Threadipper 5960X),1Gb kafka connection  , Samsung 980 pro 7/5 Gb r/w sec.
Datafile: 1.3Gb data file , 30 columns, total 528 chars (runes)  row width.

```console
rickard@Oden-Threadripper:~/GolandProjects/shredder2$ ./shredder 10.1.1.90:9092 10.1.1.90:8081 schema1.json 8 test.last111
Schema = {
"type": "record",
"name": "weblog",
"fields" : [
...
skipping footer
elapesed total= 1.658694972s

```

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
kafka/avro code origins from https://github.com/mycujoo/go-kafka-avro from mycujoo.tv "Democratizing football broadcasting."
