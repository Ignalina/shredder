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

package main

import (
	"github.com/ignalina/shredder/common"
	"github.com/ignalina/shredder/fixed2avro"
	"os"
	"strconv"
	"time"
)

func main() {

	if len(os.Args) != 8 {
		println("Shredder V1.0 2021-12-19 02:24")
		println("Syntax       : shredder <http[s]://kafkabroker | /outputdir> <schemaregistry> <schema file url> <schema id> <topic> <cores=partitions> <data file> ")
		println("example usage: shredder http://10.1.1.90:9092 10.1.1.90:8081 schema1.json 5 tableXYZ_q123 1 test.data")
		os.Exit(1)
	}

	schemaId, _ := strconv.Atoi(os.Args[4])
	cores, _ := strconv.Atoi(os.Args[6])
	fullPath_data := os.Args[7] //"test.last10"

	var fst = common.FixedSizeTable{
		Args:           os.Args,
		Schemaregistry: os.Args[2],
		SchemaFilePath: os.Args[3],
		Cores:          cores,
		SchemaID:       schemaId,
	}

	start := time.Now()

	var t = fixed2avro.Table{
		Fst: &fst,
	}

	err := t.CreateFixedSizeTableFromSlowDisk(fullPath_data, os.Args)
	if err != nil {
		panic("Nooo we have failed" + err.Error())
	}
	fixed2avro.PrintPerfomance(time.Since(start), &fst)

}
