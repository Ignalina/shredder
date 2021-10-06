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
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hamba/avro"
	"github.com/ignalina/shredder/kafkaavro"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
	"io"
	"log"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

type FixedField struct {
	Len int
	ColumnType string
}

type FixedRow struct {
	FixedField []FixedField  // For parsing
	recordStruct reflect.Type // For Avro serializing
}

type avroBinaryBytes []byte

type FixedSizeTableChunk struct {
	fixedSizeTable *FixedSizeTable
	columnBuilders []ColumnBuilder
	bytes []byte
	recordStructInstance reflect.Value
	avrobinaroValueBytes []avroBinaryBytes
	Producer *kafkaavro.Producer
	LinesParsed int
	durationReadChunk time.Duration
	durationToAvro time.Duration
	durationToKafka time.Duration
}

type FixedSizeTable struct {
	// pointer to bytebuffer
	Bytes       []byte
	TableChunks []FixedSizeTableChunk
	row              *FixedRow
	schema           *avro.Schema
	SchemaID         int
	BootstrapServers string
	Schemaregistry   string
	wg               *sync.WaitGroup
	SchemaFilePath   string
	Cores            int
	LinesParsed int
	DurationReadChunk time.Duration
	DurationToAvro time.Duration
	DurationToKafka time.Duration
}

type  ColumnBuilder interface {
	ParseValue(name string) bool
	FinishColumn() bool
}

func (f FixedRow) CalRowLength() int {
	sum := 0

	for _, num := range f.FixedField {
		sum += num.Len
	}
	return sum+2
}

//  unsigned char glyph=(unsigned char)195;
func findLastNL(bytes []byte) int {
	p2:= len(bytes)
	if(0==p2) {
		return -1
	}

	for (p2 >0)  {
		if(bytes[p2-1]==0x0d && bytes[p2]==0x0a) {
			return p2 + 1
		}
		p2--
	}

	return 0
}

func CreateSchemaFromFile(schemaFilePath string) (*avro.Schema,error) {
	f, e := os.Open(schemaFilePath)
	if e != nil {
		panic(e)
	}
	defer f.Close()
	bu := new(strings.Builder)
	io.Copy(bu, f)
	fmt.Println("Schema =", bu.String())

	avroSchema, err := avro.Parse(bu.String())
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return &avroSchema,err
}

func CreateRowFromSchema(schemaFilePath string) *FixedRow {

	var fixedRow FixedRow
	var columnLen float64
	var columnName, columnType string

	f, e := os.Open(schemaFilePath)
	if e != nil {
		panic(e)
	}
	defer f.Close()
	bu := new(strings.Builder)
	io.Copy(bu, f)


	var v interface{}
 	sf:=[]reflect.StructField{}
	ff:=[]FixedField{}

	// Unmarshal or Decode the JSON to the interface.
	json.Unmarshal([]byte(bu.String()), &v)
	data := v.(map[string]interface{})

	for k, v := range data {
		switch v := v.(type) {
		case []interface{}:

			nrOfCols:=len(v)

			ff=make([]FixedField, nrOfCols)
			sf=make([]reflect.StructField,nrOfCols)

			for i, u := range v {
				maps := u.(map[string]interface{})
				columnName = maps["name"].(string)
				maps2 := maps["type"].(map[string]interface{})
				columnLen = maps2["len"].(float64)

				for ii, uu := range maps2 {

					switch uu.(type) {
					case string:
						if ii == "type" {
							columnType = uu.(string)
						} else if ii == "logicalType" {
							columnType = uu.(string)
						}

					}
				}

				ff[i]= FixedField{
						Len:   int(columnLen),
						ColumnType: columnType,  // logical column type , for column parser factory.
					}

				sf[i]= reflect.StructField{
					Name:      strings.Title(columnName),
					Type:      getGoTypeFromAvroType(columnType),
				}

				fmt.Println(columnName)
			}

		default:
			fmt.Println(k, v, "ignored (unknown)")
		}
	}
	fixedRow.FixedField=ff
	fixedRow.recordStruct = reflect.StructOf(sf)

	return &fixedRow
}


// REMOVE
func getGoTypeFromAvroType(columnType string) reflect.Type {

	 mapping := map[string] reflect.Type {
		"boolean" : reflect.TypeOf(true),
		"Bytes": reflect.TypeOf([]byte("")),
		"float": reflect.TypeOf(float32(0)),
		"double": reflect.TypeOf(float64(0)),
		"long": reflect.TypeOf(int64(0)),
		"int": reflect.TypeOf(int32(0)),
		"string": reflect.TypeOf(string("")),
		"date": reflect.TypeOf(int32(0)),
		"time-millis": reflect.TypeOf(int32(0)),
		"time-micros": reflect.TypeOf(int64(0)),
		"timestamp-millis": reflect.TypeOf(int64(0)),
		"timestamp-micros": reflect.TypeOf(int64(0)),
	}

	return mapping[columnType]
}


func (fstc *FixedSizeTableChunk) CreateColumBuilders() bool {
	fstc.columnBuilders=make([]ColumnBuilder, len(fstc.fixedSizeTable.row.FixedField))

	var err error;


	srUrl :=url.URL{
		Scheme: "http",
		Host:   fstc.fixedSizeTable.Schemaregistry,
	}

	fstc.Producer, err = kafkaavro.NewProducer(

		"topic",
		`"string"`,
		(*fstc.fixedSizeTable.schema).String(),
		kafkaavro.WithKafkaConfig(&kafka.ConfigMap{
			"bootstrap.servers":        fstc.fixedSizeTable.BootstrapServers,
			"socket.keepalive.enable":  true,
		}),
		kafkaavro.WithSchemaRegistryURL(&srUrl),
	)
	if(nil!=err) {
		return false
	}

	v:=	reflect.New(fstc.fixedSizeTable.row.recordStruct).Elem()
	fstc.recordStructInstance=v

	for i, ff := range fstc.fixedSizeTable.row.FixedField {
		fstc.columnBuilders[i]=*CreateColumBuilder(i,&ff,ff.Len,&fstc.recordStructInstance)
	}
	return true
}
func (fstc *FixedSizeTableChunk) appendAvroBinary( ) ( error) {

	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(fstc.fixedSizeTable.SchemaID))

//	var value interface{}
//	value=fstc.recordStructInstance
	// Convert to binary Avro data
	binaryValue, err := avro.Marshal(*fstc.fixedSizeTable.schema, fstc.recordStructInstance.Addr().Interface())
	if err != nil {
		return err
	}

	binaryMsg := make([]byte, 0, len(binaryValue)+5)
	// first byte is magic byte, always 0 for now
	binaryMsg = append(binaryMsg, byte(0))
	// 4-byte schema ID as returned by the Schema Registry
	binaryMsg = append(binaryMsg, binarySchemaId...)
	// avro serialized data in Avro’s binary encoding
	binaryMsg = append(binaryMsg, binaryValue...)


	fstc.avrobinaroValueBytes=append(fstc.avrobinaroValueBytes,binaryMsg)

	return nil
}

// Read chunks of file and process them in go route after each chunk read. Slow disk is non non zero disk like sans etc
func (fst *FixedSizeTable) CreateFixedSizeTableFromSlowDisk2(fileName string) (error) {

	fst.schema,_ = CreateSchemaFromFile(fst.SchemaFilePath)
	fst.SchemaID = 1  // TODO FIX THIS !!!
	fst.row =  CreateRowFromSchema(fst.SchemaFilePath)

	fst.wg = &sync.WaitGroup {}
	ParalizeChunks(fst ,fileName)


	return nil
}
func ParalizeChunks(fst *FixedSizeTable ,filename string)  error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	fi,_:=file.Stat()

	fst.Bytes =make([]byte, fi.Size())
	fst.TableChunks=make([]FixedSizeTableChunk, fst.Cores)

	chunkSize:=fi.Size()/int64(fst.Cores)
	rowlength:=int64(fst.row.CalRowLength())

	if(chunkSize<int64(rowlength)) {
		chunkSize=int64(rowlength)
	}

	goon:=true
	chunkNr:=0
	p1:=0
	p2:=0

	for goon {
		fst.TableChunks[chunkNr]=FixedSizeTableChunk {fixedSizeTable: fst}
		fst.TableChunks[chunkNr].CreateColumBuilders()

		i1:=int(chunkSize)*chunkNr
		i2:=int(chunkSize)*(chunkNr+1)
		if(chunkNr==(fst.Cores-1)) {
			i2= len(fst.Bytes)
		}
		buf := fst.Bytes[i1:i2]
		startReadChunk:=time.Now()
		nread,_:=io.ReadFull(file,buf)
		fst.TableChunks[chunkNr].durationReadChunk=time.Since(startReadChunk)
		buf = buf[:nread]
		goon = i2<len(fst.Bytes)
		p2 = i1+findLastNL(buf)

		fst.TableChunks[chunkNr].bytes=fst.Bytes[p1:p2]
		p1=p2
		fst.wg.Add(1)
		go fst.TableChunks[chunkNr].process()

		chunkNr++
	}
	fst.wg.Wait()


// Sum up some statitics
	for _, tableChunk := range fst.TableChunks {
		fst.DurationToAvro+=tableChunk.durationToAvro
		fst.DurationReadChunk+=tableChunk.durationReadChunk
		fst.DurationToKafka+=tableChunk.durationToKafka
		fst.LinesParsed += tableChunk.LinesParsed
	}

	return nil
}
func (fstc *FixedSizeTableChunk) process()  {
	startToAvro:=time.Now()
	defer fstc.fixedSizeTable.wg.Done()
	re := bytes.NewReader(fstc.bytes)
	decodingReader := transform.NewReader(re, charmap.ISO8859_1.NewDecoder())

	scanner := bufio.NewScanner(decodingReader)
	lineCnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		if(line[:12]=="************") {
			fmt.Println("skipping footer");
			break
		}
		lineCnt++


		var columnPos int
		for ci, cc := range fstc.fixedSizeTable.row.FixedField {
			columString := line[columnPos : columnPos+cc.Len]
			fstc.columnBuilders[ci].ParseValue(columString)
			columnPos += cc.Len
		}
		fstc.appendAvroBinary()

	}
	fstc.durationToAvro=time.Since(startToAvro)
	fstc.LinesParsed=lineCnt

// send to kafka

	startToKafka:=time.Now()
	c := make(chan kafka.Event)

	for _,abv := range fstc.avrobinaroValueBytes {
		fstc.Producer.ProduceFast("string", abv,c)
	}
	fstc.durationToKafka=time.Since(startToKafka)

}


var lo = &time.Location{}


// 2020-07-09-09.59.59.99375
func  DateStringT1ToUnix(dateString string) (int64,error) {

	var year64,month64,day64,hour64,minute64,second64 int64
	var err error

	year64,err = strconv.ParseInt(dateString[:4], 10, 32)

	if(nil!=err) {
		return 0,err
	}

	month64,err = strconv.ParseInt(dateString[5:7], 10, 8)

	if(nil!=err) {
		return 0,err
	}

	day64,err = strconv.ParseInt(dateString[8:10], 10, 8)
	if(nil!=err) {
		return 0,err
	}

	hour64,err = strconv.ParseInt(dateString[11:13], 10, 8)
	if(nil!=err) {
		return 0,err
	}

	minute64,err = strconv.ParseInt(dateString[14:16], 10, 8)
	if(nil!=err) {
		return 0,err
	}

	second64,err = strconv.ParseInt(dateString[17:19], 10, 8)
	if(nil!=err) {
		return 0,err
	}

	var ti time.Time

	ti=time.Date(int(year64), time.Month(month64) , int(day64), int(hour64), int(minute64), int(second64), 0, lo)

	return ti.Unix(),nil

}

func IsError(err error) bool {
	if err != nil {
		fmt.Println(err.Error())
	}
	return (err != nil)
}

func CreateColumBuilder(fieldnr int,fixedField *FixedField ,columnsize int, 	recordStructInstance *reflect.Value) *ColumnBuilder {
	var result ColumnBuilder
	columnsize = 0
//	columnsizeCap := 3000000

	switch fixedField.ColumnType {
	case "boolean":
		result = &ColumnBuilderBoolean{fixedField: fixedField, fieldnr: fieldnr, recordStructInstance: recordStructInstance}
	case "Bytes":
		result = &ColumnBuilderBytes{fixedField: fixedField, fieldnr: fieldnr, recordStructInstance: recordStructInstance}
	case "float":
		result = &ColumnBuilderFloat{fixedField: fixedField, fieldnr: fieldnr, recordStructInstance: recordStructInstance}
	case "double":
		result = &ColumnBuilderDouble{fixedField: fixedField, fieldnr: fieldnr, recordStructInstance: recordStructInstance}

	case "int":
		result = &ColumnBuilderInt{fixedField: fixedField, fieldnr: fieldnr, recordStructInstance: recordStructInstance}
	case "string":
		result = &ColumnBuilderString{fixedField: fixedField, fieldnr: fieldnr, recordStructInstance: recordStructInstance}
	case "long":
		result = &ColumnBuilderLong{fixedField: fixedField, fieldnr: fieldnr, recordStructInstance: recordStructInstance}
	case "date":
		result = &ColumnBuilderDate{fixedField: fixedField, fieldnr: fieldnr, recordStructInstance: recordStructInstance}
	case "timestamp-millis":
		result = &ColumnBuilderTimestapMillis{fixedField: fixedField, fieldnr: fieldnr, recordStructInstance: recordStructInstance}
	case "timestamp-micros":
		result = &ColumnBuilderTimestapMicros{fixedField: fixedField, fieldnr: fieldnr, recordStructInstance: recordStructInstance}

	default:
		fmt.Printf("Unknown type ",fixedField.ColumnType )

	}

	return &result
}

