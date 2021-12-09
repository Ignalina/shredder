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
	"reflect"
	"strconv"
)

type ColumnBuilderBoolean struct {
	fixedField           *FixedField
	fieldnr              int
	recordStructInstance *reflect.Value
}

// make configurable
func (c *ColumnBuilderBoolean) ParseValue(name string) bool {
	boolChar := name[0]
	var ourBool bool

	switch boolChar {
	case uint8('J'):
		ourBool = true
		break

	case uint8('j'):
		ourBool = true
		break

	case uint8('Y'):
		ourBool = true
		break
	case uint8('y'):
		ourBool = true
		break

	case uint8('N'):
		ourBool = false
		break

	case uint8('n'):
		ourBool = false
		break
	}
	c.recordStructInstance.Field(c.fieldnr).SetBool(ourBool)

	return true
}
func (c *ColumnBuilderBoolean) FinishColumn() bool {
	return true
}

type ColumnBuilderBytes struct {
	fixedField           *FixedField
	fieldnr              int
	recordStructInstance *reflect.Value
}

func (c ColumnBuilderBytes) ParseValue(name string) bool {
	c.recordStructInstance.Field(c.fieldnr).SetBytes([]byte(name))
	return true
}

func (c ColumnBuilderBytes) FinishColumn() bool {
	return true
}

type ColumnBuilderDouble struct {
	fixedField           *FixedField
	fieldnr              int
	recordStructInstance *reflect.Value
}

func (c ColumnBuilderDouble) ParseValue(name string) bool {
	floatNum, err := strconv.ParseFloat(name, 64)
	c.recordStructInstance.Field(c.fieldnr).SetFloat(floatNum)
	return (nil == err)
}

func (c ColumnBuilderDouble) FinishColumn() bool {
	return true
}

type ColumnBuilderFloat struct {
	fixedField           *FixedField
	fieldnr              int
	recordStructInstance *reflect.Value
}

func (c ColumnBuilderFloat) ParseValue(name string) bool {
	floatNum, err := strconv.ParseFloat(name, 32)
	c.recordStructInstance.Field(c.fieldnr).SetFloat(floatNum)
	return (nil == err)
}

func (c ColumnBuilderFloat) FinishColumn() bool {
	return true
}

type ColumnBuilderLong struct {
	fixedField           *FixedField
	fieldnr              int
	recordStructInstance *reflect.Value
}

func (c ColumnBuilderLong) ParseValue(name string) bool {
	longNum, err := strconv.ParseInt(name, 10, 64)

	c.recordStructInstance.Field(c.fieldnr).SetInt(longNum)
	return (nil == err)
}

func (c ColumnBuilderLong) FinishColumn() bool {
	return true
}

type ColumnBuilderInt struct {
	fixedField           *FixedField
	fieldnr              int
	recordStructInstance *reflect.Value
}

func (c ColumnBuilderInt) ParseValue(name string) bool {
	intNum, err := strconv.ParseInt(name, 10, 32)
	c.recordStructInstance.Field(c.fieldnr).SetInt(intNum)
	return (nil == err)
}

func (c ColumnBuilderInt) FinishColumn() bool {
	return true
}

type ColumnBuilderString struct {
	fixedField           *FixedField
	fieldnr              int
	recordStructInstance *reflect.Value
}

func (c ColumnBuilderString) ParseValue(name string) bool {
	c.recordStructInstance.Field(c.fieldnr).SetString(name)
	return true
}

func (c ColumnBuilderString) FinishColumn() bool {
	return true
}

type ColumnBuilderDate struct {
	fixedField           *FixedField
	fieldnr              int
	recordStructInstance *reflect.Value
}

func (c ColumnBuilderDate) ParseValue(name string) bool {

	f, err := DateStringT1ToUnix_microsecond(name)

	if err == nil {
		return false
	}
	c.recordStructInstance.Field(c.fieldnr).SetInt(f)
	return true
}

func (c ColumnBuilderDate) FinishColumn() bool {
	return true
}

type ColumnBuilderTimestapMillis struct {
	fixedField           *FixedField
	fieldnr              int
	recordStructInstance *reflect.Value
}

func (c ColumnBuilderTimestapMillis) ParseValue(name string) bool {

	f, err := DateStringT1ToUnix_millisecond(name)

	if err == nil {
		return false
	}
	c.recordStructInstance.Field(c.fieldnr).SetInt(f)
	return true
}

func (c ColumnBuilderTimestapMillis) FinishColumn() bool {
	return true
}

type ColumnBuilderTimestapMicros struct {
	fixedField           *FixedField
	fieldnr              int
	recordStructInstance *reflect.Value
}

func (c ColumnBuilderTimestapMicros) ParseValue(name string) bool {

	f, err := DateStringT1ToUnix_microsecond(name)

	if err != nil {
		return false
	}
	c.recordStructInstance.Field(c.fieldnr).SetInt(f)
	return true
}

func (c ColumnBuilderTimestapMicros) FinishColumn() bool {
	return true
}
