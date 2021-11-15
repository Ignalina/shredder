/*
 * MIT No Attribution
 *
 * Copyright 2021 Rickard Lundin (rickard@ignalina.dk)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package impl

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ignalina/shredder/kafkaavro"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type  ExportProducer interface {
    Setup() error
	Export() error
	Finish() error
}


type KafkaExporter struct {
	Fstc *FixedSizeTableChunk
	BootstrapServers string
	Topic             string
	producer             *kafkaavro.Producer
	C                    chan kafka.Event
}

func (ep *KafkaExporter) Setup() error {
	srUrl :=url.URL{
		Scheme: "http",
		Host:   ep.Fstc.fixedSizeTable.Schemaregistry,
		}

	var err error

	ep.producer, err = kafkaavro.NewProducer(
		ep.Topic,
		ep.Fstc.chunkr ,
		`"string"`,
		(*ep.Fstc.fixedSizeTable.schema).String(),
		kafkaavro.WithKafkaConfig(&kafka.ConfigMap{
			"bootstrap.servers":        ep.BootstrapServers,
			"socket.keepalive.enable":  true,
		}),
		kafkaavro.WithSchemaRegistryURL(&srUrl),
	)

	return err
}

func (ep *KafkaExporter) Export() error {
	// send to kafka. NOTE: This could have been been started in earlier loop...
	//
	c := make(chan kafka.Event)
	ep.C=c
	for _,abv := range ep.Fstc.avrobinaroValueBytes {
		ep.producer.ProduceFast("string", abv,c)
	}

	return nil
}


func (ep *KafkaExporter) Finish() error {
	// Wait for a successfull kafka transmission

	e := <-ep.C
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return m.TopicPartition.Error
	}
	return nil
}

type AvroFileExporter struct {
	Fstc     *FixedSizeTableChunk
	FileName string
	file     *os.File
}

func (ep *AvroFileExporter) Setup() error {

	f, err := os.OpenFile(ep.FileName+strconv.Itoa(ep.Fstc.chunkr), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	ep.file=f

	return nil
}

func (ep *AvroFileExporter) Export() error {


	for _,abv := range ep.Fstc.avrobinaroValueBytes {
		_,err:=ep.file.Write(abv[5:])
		if(nil!=err) {
			return err
		}
	}

	return nil
}

func (ep *AvroFileExporter) Finish() error {
	ep.file.Close()

	return nil
}


func ExportersFactory(args []string, chunk *FixedSizeTableChunk) *ExportProducer {
	var ptrExportProducer ExportProducer
	var httpType bool
	var proto string
	var url string

	url=args[1]
	httpType,proto=extractHttpPrefix(url)

	if (httpType) {
		var ip string

		ip=strings.TrimPrefix(url,proto)

		ptrExportProducer = &KafkaExporter{
			BootstrapServers: ip,
			Topic:            os.Args[5],
			Fstc:             chunk,
		}
	} else if(!httpType) {
		ptrExportProducer = &AvroFileExporter{
			Fstc:             chunk,
			FileName: url,
		}

	}

	return &ptrExportProducer


}

func extractHttpPrefix(myString string) (bool,string) {
	var proto string
	var theType bool

	if(strings.HasPrefix(myString,"http://")) {
		proto="http://"
		theType=true
	} else if(strings.HasPrefix(myString,"https://" )) {
		proto="https://"
		theType=true
	}

	return theType,proto
}

