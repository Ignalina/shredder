package impl

import (
	"fmt"
	"github.com/inhies/go-bytesize"
	"time"
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

		for bytePos, _ := range fullString[firstByte:lastByte] {
			runeLen++
			if (runeLen==s.runeLen) {
				substring[is].sub = fullString[firstByte:firstByte+bytePos+1]
				firstByte=firstByte+bytePos+1
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
	fmt.Println("Time spent toKafka      :",fst.DurationToKafka.Seconds()/fcores,"s")
	fmt.Println("Time spent DoneKafka      :",fst.DurationDoneKafka.Seconds(),"s")



}
