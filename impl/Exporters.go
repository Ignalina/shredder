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
	fstc *FixedSizeTableChunk
}

func (ep *AvroFileExporter) Setup() error {
	return nil
}

func (ep *AvroFileExporter) Export() error {
	return nil
}

func (ep *AvroFileExporter) Finish() error {
	return nil
}


func ExportersFactory(args []string, chunk *FixedSizeTableChunk) *ExportProducer {
	var ptrExportProducer ExportProducer
	ptrExportProducer = &KafkaExporter{
		BootstrapServers:args[1] ,
		Topic: os.Args[5],
		Fstc: chunk,

	}

	return &ptrExportProducer


}


