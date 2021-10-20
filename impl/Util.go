package impl

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
