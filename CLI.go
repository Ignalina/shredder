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
	"fmt"
	"github.com/ignalina/shredder/impl"
	"github.com/inhies/go-bytesize"
	"os"
	"strconv"
	"time"
)

func main() {

	start := time.Now()
    if(len(os.Args)!=7) {
		println("Syntax       : shredder.exe <kafka broker> <schemaregistry> <avro schema> <schema id> <cores> <data file> ")
		println("example usage: shredder.exe 10.1.1.90:9092 10.1.1.90:8081 schema1.json 5 test.data 2")
		os.Exit(1)
	}

	schemaId,_:=strconv.Atoi(os.Args[4])
	cores,_:= strconv.Atoi(os.Args[5])
	fullPath_data := os.Args[6]  //"test.last10"

	var fst = impl.FixedSizeTable{
		SchemaID:         schemaId,
		Schemaregistry:   os.Args[2],
		SchemaFilePath:   os.Args[3],
		Cores:            cores,
	}

	err:= fst.CreateFixedSizeTableFromSlowDisk2(fullPath_data)
	if( err!=nil) {
		panic("Nooo we have failed")
	}

	printPerfomance(time.Since(start),&fst)



}

func printPerfomance( elapsed  time.Duration,fst *impl.FixedSizeTable) {

	fcores := float64(fst.Cores)
	var tpb = bytesize.New(float64(len(fst.Bytes)) / float64(elapsed.Seconds()))
	var tpl = bytesize.New(float64(fst.LinesParsed) / float64(elapsed.Seconds()))
	tpls := tpl.String()[:len(tpl.String())-1]
	toAvro:=fst.DurationToAvro.Seconds()/fcores
	tpal :=bytesize.New(float64(fst.LinesParsed)/ toAvro)
	tpals :=tpal.String()[:len(tpal.String())-1]

	fmt.Println("Time spend in total     :", elapsed," parsing ",fst.LinesParsed," lines from ", len(fst.Bytes)," bytes")

	fmt.Println("Troughput bytes/s total :", tpb,"/s")
	fmt.Println("Troughput lines/s total :", tpls," Lines/s")
	fmt.Println("Troughput lines/s toAvro:", tpals ," Lines/s")



	fmt.Println("Time spent toReadChunks :",fst.DurationReadChunk.Seconds()/fcores,"s")
	fmt.Println("Time spent toAvro       :",toAvro,"s")
	fmt.Println("Time spent toKafka      :",fst.DurationToKafka.Seconds()/fcores,"s")



}

