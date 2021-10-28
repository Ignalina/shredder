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
	"github.com/ignalina/shredder/impl"
	"os"
	"strconv"
	"time"
)

func main() {

	start := time.Now()
        if(len(os.Args)!=8) {        
		println("Shredder (Unreleased Betha 2021-10-21 17:39)")
		println("Syntax       : shredder.exe <kafka broker> <schemaregistry> <schema file url> <schema id> <topic> <cores=partitions> <data file> ")
		println("example usage: shredder.exe 10.1.1.90:9092 10.1.1.90:8081 schema1.json 5 tableXYZ_q123 1 test.data")
		os.Exit(1)
	}
	
	schemaId,_:=strconv.Atoi(os.Args[4])
	cores,_:= strconv.Atoi(os.Args[6])
	fullPath_data := os.Args[7]  //"test.last10"

	var fst = impl.FixedSizeTable{
		BootstrapServers: os.Args[1],
		Schemaregistry:   os.Args[2],
		SchemaFilePath:   os.Args[3],
		Cores:            cores,
		Topic: os.Args[5],
		SchemaID:         schemaId,
	}

	err:= fst.CreateFixedSizeTableFromSlowDisk(fullPath_data)
	if( err!=nil) {
		panic("Nooo we have failed"+err.Error())
	}

	impl.PrintPerfomance(time.Since(start),&fst)

}


