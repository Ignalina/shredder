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
	"github.com/hamba/avro"
	"log"
	"reflect"
)

func CreateSchema(schemaAsString string) (*avro.Schema, error) {

	avroSchema, err := avro.Parse(schemaAsString)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return &avroSchema, err
}

func getGoTypeFromAvroType(columnType string) reflect.Type {

	mapping := map[string]reflect.Type{
		"boolean":          reflect.TypeOf(true),
		"Bytes":            reflect.TypeOf([]byte("")),
		"float":            reflect.TypeOf(float32(0)),
		"double":           reflect.TypeOf(float64(0)),
		"long":             reflect.TypeOf(int64(0)),
		"int":              reflect.TypeOf(int32(0)),
		"string":           reflect.TypeOf(string("")),
		"date":             reflect.TypeOf(int32(0)),
		"time-millis":      reflect.TypeOf(int32(0)),
		"time-micros":      reflect.TypeOf(int64(0)),
		"timestamp-millis": reflect.TypeOf(int64(0)),
		"timestamp-micros": reflect.TypeOf(int64(0)),
	}

	return mapping[columnType]
}
