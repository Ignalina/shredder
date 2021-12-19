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

package common

import (
	"encoding/json"
	"github.com/hamba/avro"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"
)

type FixedField struct {
	Len        int
	ColumnType string
}

type FixedRow struct {
	FixedField   []FixedField // For parsing
	RecordStruct reflect.Type // For Avro serializing
}

func (f FixedRow) CalRowLength() int {
	sum := 0

	for _, num := range f.FixedField {
		sum += num.Len
	}
	return sum + 2
}

type avroBinaryBytes []byte

type FixedSizeTableChunk struct {
	Chunkr               int
	FixedSizeTable       *FixedSizeTable
	Bytes                []byte
	RecordStructInstance reflect.Value
	AvrobinaroValueBytes []avroBinaryBytes

	LinesParsed       int
	DurationReadChunk time.Duration
	DurationToAvro    time.Duration
	DurationToExport  time.Duration
}

type FixedSizeTable struct {
	Args               []string
	Bytes              []byte
	TableChunks        []FixedSizeTableChunk
	Row                *FixedRow
	Schema             *avro.Schema
	SchemaAsString     string
	SchemaID           int
	Schemaregistry     string
	Wg                 *sync.WaitGroup
	SchemaFilePath     string
	Cores              int
	LinesParsed        int
	DurationReadChunk  time.Duration
	DurationToAvro     time.Duration
	DurationToExport   time.Duration
	DurationDoneExport time.Duration
	BinarySchemaId     []byte
}

func CreateRowFromSchema(schemaAsString string) (*FixedRow, error) {

	var fixedRow FixedRow
	var columnLen float64
	var columnName, columnType string

	var v interface{}
	sf := []reflect.StructField{}
	ff := []FixedField{}

	// Unmarshal or Decode the JSON to the interface.
	json.Unmarshal([]byte(schemaAsString), &v)
	data := v.(map[string]interface{})

	for k, v := range data {
		switch v := v.(type) {
		case []interface{}:

			nrOfCols := len(v)

			ff = make([]FixedField, nrOfCols)
			sf = make([]reflect.StructField, nrOfCols)

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

				ff[i] = FixedField{
					Len:        int(columnLen),
					ColumnType: columnType, // logical column type , for column parser factory.
				}

				sf[i] = reflect.StructField{
					Name: strings.Title(columnName),
					Type: getGoTypeFromAvroType(columnType),
				}

			}

		default:
			log.Println("ignored (unknown)", k, v)
		}
	}
	fixedRow.FixedField = ff
	fixedRow.RecordStruct = reflect.StructOf(sf)

	return &fixedRow, nil
}

func FindLastNL(bytes []byte) int {
	p2 := len(bytes)
	if 0 == p2 {
		return -1
	}

	for p2 > 2 {
		if bytes[p2-2] == 0x0d && bytes[p2-1] == 0x0a {
			return p2
		}
		p2--
	}

	return 0
}
