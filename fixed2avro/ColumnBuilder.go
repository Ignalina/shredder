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
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/hamba/avro"
	"github.com/ignalina/shredder/common"
	"io"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"
)

type TableChunk struct {
	fstc           *common.FixedSizeTableChunk
	Table          *Table
	columnBuilders []ColumnBuilder
	Exporter       ExportProducer
}

type Table struct {
	Fst         *common.FixedSizeTable
	TableChunks []TableChunk
}

type ColumnBuilder interface {
	ParseValue(name string) bool
	FinishColumn() bool
}

func (tb *TableChunk) CreateColumBuilders() bool {

	tb.columnBuilders = make([]ColumnBuilder, len(tb.fstc.FixedSizeTable.Row.FixedField))

	var err error

	err = tb.Exporter.Setup()
	//		Exporter[tb.fstc.Chunkr].Setup()

	if nil != err {
		return false
	}

	v := reflect.New(tb.fstc.FixedSizeTable.Row.RecordStruct).Elem()
	tb.fstc.RecordStructInstance = v

	for i, ff := range tb.fstc.FixedSizeTable.Row.FixedField {
		tb.columnBuilders[i] = *CreateColumBuilder(i, &ff, ff.Len, &tb.fstc.RecordStructInstance)
	}
	return true
}
func (tb *TableChunk) appendAvroBinary() error {

	// TODO: what if we could have the marhsalling done to a specifed array with the first 5 bytes with the header...

	binaryValue, err := avro.Marshal(*tb.fstc.FixedSizeTable.Schema, tb.fstc.RecordStructInstance.Addr().Interface())
	if err != nil {
		return err
	}

	binaryMsg := make([]byte, 0, len(binaryValue)+5)
	// first byte is magic byte, always 0 for now
	binaryMsg = append(binaryMsg, byte(0))
	// 4-byte schema ID as returned by the Schema Registry
	binaryMsg = append(binaryMsg, tb.fstc.FixedSizeTable.BinarySchemaId...)
	// avro serialized data in Avro’s binary encoding
	binaryMsg = append(binaryMsg, binaryValue...)

	tb.fstc.AvrobinaroValueBytes = append(tb.fstc.AvrobinaroValueBytes, binaryMsg)

	return nil
}

// Read chunks of file and process them in go routine after each chunk read. Slow disk is non non zerocopy disk like sans etc
func (t *Table) CreateFixedSizeTableFromSlowDisk(fileName string, args []string) error {
	var err error

	t.Fst.SchemaAsString, err = common.ReadFileToString(t.Fst.SchemaFilePath)
	t.Fst.Schema, err = common.CreateSchema(t.Fst.SchemaAsString)
	if nil != err {
		return err
	}
	t.Fst.BinarySchemaId = make([]byte, 4)
	binary.BigEndian.PutUint32(t.Fst.BinarySchemaId, uint32(t.Fst.SchemaID))
	if err != nil {
		return err
	}

	t.Fst.Row, err = common.CreateRowFromSchema(t.Fst.SchemaAsString)
	if nil != err {
		return err
	}

	t.Fst.Wg = &sync.WaitGroup{}
	return ParalizeChunks(t, fileName, args)

}

func ParalizeChunks(t *Table, filename string, args []string) error {

	file, err := os.Open(filename)

	if err != nil {
		return err
	}
	defer file.Close()
	fi, _ := file.Stat()

	t.Fst.Bytes = make([]byte, fi.Size())
	t.Fst.TableChunks = make([]common.FixedSizeTableChunk, t.Fst.Cores)
	t.TableChunks = make([]TableChunk, t.Fst.Cores)

	chunkSize := fi.Size() / int64(t.Fst.Cores)
	rowlength := int64(t.Fst.Row.CalRowLength())

	if chunkSize < int64(rowlength) {
		chunkSize = int64(rowlength)
	}

	goon := true
	chunkNr := 0
	p1 := 0
	p2 := 0

	for goon {

		t.Fst.TableChunks[chunkNr] = common.FixedSizeTableChunk{FixedSizeTable: t.Fst, Chunkr: chunkNr}
		t.TableChunks[chunkNr] = TableChunk{fstc: &t.Fst.TableChunks[chunkNr], Table: t}
		t.TableChunks[chunkNr].Exporter = *ExportersFactory(os.Args, &t.Fst.TableChunks[chunkNr])
		t.TableChunks[chunkNr].CreateColumBuilders()

		i1 := int(chunkSize) * chunkNr
		i2 := int(chunkSize) * (chunkNr + 1)
		if chunkNr == (t.Fst.Cores - 1) {
			i2 = len(t.Fst.Bytes)
		}
		buf := t.Fst.Bytes[i1:i2]
		startReadChunk := time.Now()
		nread, _ := io.ReadFull(file, buf)
		t.Fst.TableChunks[chunkNr].DurationReadChunk = time.Since(startReadChunk)
		buf = buf[:nread]
		goon = i2 < len(t.Fst.Bytes)
		p2 = i1 + common.FindLastNL(buf)

		t.Fst.TableChunks[chunkNr].Bytes = t.Fst.Bytes[p1:p2]
		p1 = p2
		t.Fst.Wg.Add(1)
		t.TableChunks[chunkNr].process()
		chunkNr++
	}

	t.Fst.Wg.Wait() // Waiting for ALL pararell routes to finish

	// Sum up some statitics
	for _, tableChunk := range t.Fst.TableChunks {
		t.Fst.DurationToAvro += tableChunk.DurationToAvro
		t.Fst.DurationReadChunk += tableChunk.DurationReadChunk
		t.Fst.DurationToExport += tableChunk.DurationToExport
		t.Fst.LinesParsed += tableChunk.LinesParsed
	}

	startWaitDoneExport := time.Now()

	for i, _ := range t.Fst.TableChunks {
		err := t.TableChunks[i].Exporter.Finish()
		if nil != err {
			return err
		}
	}
	t.Fst.DurationDoneExport = time.Since(startWaitDoneExport)

	return nil
}

func (tb *TableChunk) process() {
	startToAvro := time.Now()
	defer tb.fstc.FixedSizeTable.Wg.Done()
	re := bytes.NewReader(tb.fstc.Bytes)
	//	decodingReader := transform.NewReader(re, charmap.ISO8859_1.NewDecoder())

	scanner := bufio.NewScanner(re)

	substring := createSubstring(tb.fstc.FixedSizeTable)

	lineCnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 12 && line[:12] == "************" {
			fmt.Println("skipping footer")
			break
		}
		lineCnt++

		getSplitBytePositions(line, substring)

		for ci, _ := range tb.fstc.FixedSizeTable.Row.FixedField {
			tb.columnBuilders[ci].ParseValue(substring[ci].sub)
		}
		tb.Exporter.ExportRow()
	}
	tb.fstc.LinesParsed = lineCnt
	tb.fstc.DurationToAvro = time.Since(startToAvro)

}

var lo = &time.Location{}

// 2020-07-09-09.59.59.99375
func DateStringT1ToUnix_millisecond(dateString string) (int64, error) {

	var year64, month64, day64, hour64, minute64, second64, nanoSec64 int64
	var err error

	year64, err = strconv.ParseInt(dateString[:4], 10, 32)

	if nil != err {
		return 0, err
	}

	month64, err = strconv.ParseInt(dateString[5:7], 10, 8)

	if nil != err {
		return 0, err
	}

	day64, err = strconv.ParseInt(dateString[8:10], 10, 8)
	if nil != err {
		return 0, err
	}

	hour64, err = strconv.ParseInt(dateString[11:13], 10, 8)
	if nil != err {
		return 0, err
	}

	minute64, err = strconv.ParseInt(dateString[14:16], 10, 8)
	if nil != err {
		return 0, err
	}

	second64, err = strconv.ParseInt(dateString[17:19], 10, 8)
	if nil != err {
		return 0, err
	}

	nanoSec64, err = strconv.ParseInt(dateString[20:23], 10, 8)
	nanoSec64 = nanoSec64 * 1000000
	if nil != err {
		return 0, err
	}

	var ti time.Time

	ti = time.Date(int(year64), time.Month(month64), int(day64), int(hour64), int(minute64), int(second64), int(nanoSec64), lo)

	return ti.Unix(), nil

}

// 2020-07-09-09.59.59.99375
func DateStringT1ToUnix_microsecond(dateString string) (int64, error) {

	var year64, month64, day64, hour64, minute64, second64, nanoSec64 int64
	var err error

	year64, err = strconv.ParseInt(dateString[:4], 10, 32)

	if nil != err {
		return 0, err
	}

	month64, err = strconv.ParseInt(dateString[5:7], 10, 8)

	if nil != err {
		return 0, err
	}

	day64, err = strconv.ParseInt(dateString[8:10], 10, 8)
	if nil != err {
		return 0, err
	}

	hour64, err = strconv.ParseInt(dateString[11:13], 10, 8)
	if nil != err {
		return 0, err
	}

	minute64, err = strconv.ParseInt(dateString[14:16], 10, 8)
	if nil != err {
		return 0, err
	}

	second64, err = strconv.ParseInt(dateString[17:19], 10, 8)
	if nil != err {
		return 0, err
	}

	nanoSec64, err = strconv.ParseInt(dateString[20:26], 10, 32)
	if nil != err {
		return 0, err
	}

	var ti time.Time

	ti = time.Date(int(year64), time.Month(month64), int(day64), int(hour64), int(minute64), int(second64), int(nanoSec64), lo)

	return ti.Unix(), nil

}

func DateStringT1ToUnix_nanosecond(dateString string) (int64, error) {

	var year64, month64, day64, hour64, minute64, second64, nanoSec64 int64
	var err error

	year64, err = strconv.ParseInt(dateString[:4], 10, 32)

	if nil != err {
		return 0, err
	}

	month64, err = strconv.ParseInt(dateString[5:7], 10, 8)

	if nil != err {
		return 0, err
	}

	day64, err = strconv.ParseInt(dateString[8:10], 10, 8)
	if nil != err {
		return 0, err
	}

	hour64, err = strconv.ParseInt(dateString[11:13], 10, 8)
	if nil != err {
		return 0, err
	}

	minute64, err = strconv.ParseInt(dateString[14:16], 10, 8)
	if nil != err {
		return 0, err
	}

	second64, err = strconv.ParseInt(dateString[17:19], 10, 8)
	if nil != err {
		return 0, err
	}

	nanoSec64, err = strconv.ParseInt(dateString[20:29], 10, 8)

	if nil != err {
		return 0, err
	}

	var ti time.Time

	ti = time.Date(int(year64), time.Month(month64), int(day64), int(hour64), int(minute64), int(second64), int(nanoSec64), lo)

	return ti.Unix(), nil

}

func IsError(err error) bool {
	if err != nil {
		fmt.Println(err.Error())
	}
	return (err != nil)
}

func CreateColumBuilder(fieldnr int, fixedField *common.FixedField, columnsize int, recordStructInstance *reflect.Value) *ColumnBuilder {
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
		fmt.Printf("Unknown type ", fixedField.ColumnType)

	}

	return &result
}
