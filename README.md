# shredder
shredds Fixed column file to avro/kafka  
Speed around 220mb/sec per Core. where 4 core  fills 1000meg/s bandwith to Kafka

Notes:
* Currently avro deserializing dislikes the data in kafka :) When this is fixed we have a 1.0 version :)
* Multicore implementation.
* Currently fixed/supported input format is 8859-1 and utf8 output
 
# Example 
Hardware: 6 core (Amd Threadipper 5960),1Gb kafka connection  
Datafile: 1.3Gb data file , 30 columns, total 528 chars (runes)  row width)

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



# Credits
kafka/avro code origins from https://github.com/mycujoo/go-kafka-avro from mycujoo.tv "Democratizing football broadcasting."
