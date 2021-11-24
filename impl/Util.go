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
	"fmt"
	"github.com/inhies/go-bytesize"
	"time"
	"unicode/utf8"
)

type Substring struct {
	runeLen int
	sub string
}

func createSubstring(fst *FixedSizeTable) ([]Substring) {

	substring := make([]Substring, len(fst.row.FixedField))
	for ci, cc := range fst.row.FixedField {
		substring[ci].runeLen=cc.Len
	}

	return substring
}

func  getSplitBytePositions(fullString string,substring []Substring)  {

	var firstByte,lastByte int
    lastByte = len(fullString)

	for is ,s := range substring {

		var runeLen int

		for bytePos, runan := range fullString[firstByte:lastByte] {
			runeLen++
			if (runeLen==s.runeLen) {
                pos:=firstByte+bytePos+utf8.RuneLen(runan)
				substring[is].sub = fullString[firstByte:pos]
				firstByte=pos
				break
			}
		}


	}
}
func PrintPerfomance( elapsed  time.Duration,fst *FixedSizeTable) {

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
	fmt.Println("Time spent WaitDoneExport      :",fst.DurationDoneExport.Seconds(),"s")

}
