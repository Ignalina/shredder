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

package fixed2avro

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hamba/avro"
	"github.com/hamba/avro/ocf"
	"github.com/ignalina/shredder/common"
	"github.com/ignalina/shredder/kafkaavro"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type ExportProducer interface {
	Setup() error
	ExportRow() error
	Finish() error
}

type KafkaExporter struct {
	Fstc             *common.FixedSizeTableChunk
	BootstrapServers string
	Topic            string
	producer         *kafkaavro.Producer
	C                chan kafka.Event
}

func (ep *KafkaExporter) Setup() error {
	srUrl := url.URL{
		Scheme: "http",
		Host:   ep.Fstc.FixedSizeTable.Schemaregistry,
	}

	var err error

	ep.producer, err = kafkaavro.NewProducer(
		ep.Topic,
		ep.Fstc.Chunkr,
		`"string"`,
		(*ep.Fstc.FixedSizeTable.Schema).String(),
		kafkaavro.WithKafkaConfig(&kafka.ConfigMap{
			"bootstrap.servers":       ep.BootstrapServers,
			"socket.keepalive.enable": true,
		}),
		kafkaavro.WithSchemaRegistryURL(&srUrl),
	)

	ep.C = make(chan kafka.Event)

	return err
}

func (ep *KafkaExporter) ExportRow() error {

	binaryValue, err := avro.Marshal(*ep.Fstc.FixedSizeTable.Schema, ep.Fstc.RecordStructInstance.Addr().Interface())
	if err != nil {
		return err
	}

	binaryMsg := make([]byte, 0, len(binaryValue)+5)
	// first byte is magic byte, always 0 for now
	binaryMsg = append(binaryMsg, byte(0))
	// 4-byte schema ID as returned by the Schema Registry
	binaryMsg = append(binaryMsg, ep.Fstc.FixedSizeTable.BinarySchemaId...)
	// avro serialized data in Avro’s binary encoding
	binaryMsg = append(binaryMsg, binaryValue...)

	ep.producer.ProduceFast("string", binaryMsg, ep.C)

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
	Fstc     *common.FixedSizeTableChunk
	FileName string
	file     *os.File
	enc      *ocf.Encoder
}

func (ep *AvroFileExporter) Setup() error {

	f, err := os.OpenFile(ep.FileName+strconv.Itoa(ep.Fstc.Chunkr), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	ep.file = f
	// TODO snappy should be configurable
	ep.enc, err = ocf.NewEncoder(ep.Fstc.FixedSizeTable.SchemaAsString, ep.file, ocf.WithCodec(ocf.Snappy))

	return err
}

func (ep *AvroFileExporter) ExportRow() error {

	ep.enc.Encode(ep.Fstc.RecordStructInstance.Addr().Interface())

	return nil
}

func (ep *AvroFileExporter) Finish() error {
	ep.enc.Flush()
	ep.enc.Close()
	ep.file.Close()

	return nil
}

func ExportersFactory(args []string, chunk *common.FixedSizeTableChunk) *ExportProducer {
	var ptrExportProducer ExportProducer
	var httpType bool
	var proto string
	var url string

	url = args[1]
	httpType, proto = extractHttpPrefix(url)

	if httpType {
		var ip string

		ip = strings.TrimPrefix(url, proto)

		ptrExportProducer = &KafkaExporter{
			BootstrapServers: ip,
			Topic:            os.Args[5],
			Fstc:             chunk,
		}
	} else if !httpType {
		ptrExportProducer = &AvroFileExporter{
			Fstc:     chunk,
			FileName: url,
		}

	}

	return &ptrExportProducer

}

func extractHttpPrefix(myString string) (bool, string) {
	var proto string
	var theType bool

	if strings.HasPrefix(myString, "http://") {
		proto = "http://"
		theType = true
	} else if strings.HasPrefix(myString, "https://") {
		proto = "https://"
		theType = true
	}

	return theType, proto
}
