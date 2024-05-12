package types

import (
	"bytes"
	"encoding/binary"
	"math"
)

type Typ interface {
	Encode() ([]byte, error)
	Decode([]byte) error
	Type() DType
	Iface() any
}

type Int int64

func (i *Int) Iface() any {
	return int64(*i)
}

func (i *Int) Type() DType {
	return IntType
}

type Float float64

func (f *Float) Iface() any {
	return float64(*f)
}

func (f *Float) Type() DType {
	return FloatType
}

type Ints []int64
type String string
type Strings []string

type DType int

// do not change sequence, as this is persisted to file
const (
	StringType DType = iota + 1
	IntType
	FloatType
	IntsType
	StringsType
)

func (dt *DType) MarshalJSON() ([]byte, error) {
	switch *dt {
	case StringType:
		return []byte(`string`), nil
	case IntType:
		return []byte(`int`), nil
	case FloatType:
		return []byte(`float`), nil
	case IntsType:
		return []byte(`ints`), nil
	case StringsType:
		return []byte(`strings`), nil
	default:
		return []byte(`string`), nil
	}
}

func (dt *DType) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case `string`:
		*dt = StringType
	case `int`:
		*dt = IntType
	case `float`:
		*dt = FloatType
	case `ints`:
		*dt = IntsType
	case `strings`:
		*dt = StringsType
	default:
		*dt = StringType
	}
	return nil
}

func (dt *DType) String() string {
	switch *dt {
	case StringType:
		return `string`
	case IntType:
		return `int`
	case FloatType:
		return `float`
	case IntsType:
		return `ints`
	case StringsType:
		return `strings`
	default:
		return `string`
	}
}

func (i *Int) Encode() ([]byte, error) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(*i))
	return b, nil
}

func (i *Int) Decode(b []byte) error {
	*i = Int(binary.LittleEndian.Uint64(b))
	return nil
}

func (f *Float) Encode() ([]byte, error) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(float64(*f)))
	// binary.LittleEndian.PutUint64(b, uint64(*f))
	return b, nil
}

func (f *Float) Decode(b []byte) error {
	*f = Float(binary.LittleEndian.Uint64(b))
	return nil
}

func (a *Ints) Encode() ([]byte, error) {
	var buf bytes.Buffer
	intbuffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(intbuffer, uint64(len(*a)))
	buf.Write(intbuffer)
	for _, intValue := range *a {
		binary.LittleEndian.PutUint64(intbuffer, uint64(intValue))
		buf.Write(intbuffer)
	}
	return buf.Bytes(), nil
}

func (a *Ints) Decode(b []byte) error {
	buf := bytes.NewBuffer(b)
	intbuffer := make([]byte, 8)
	_, err := buf.Read(intbuffer)
	if err != nil {
		return err
	}
	length := binary.LittleEndian.Uint64(intbuffer)
	*a = make([]int64, length)
	for i := 0; i < int(length); i++ {
		_, err := buf.Read(intbuffer)
		if err != nil {
			return err
		}
		(*a)[i] = int64(binary.LittleEndian.Uint64(intbuffer))
	}
	return nil
}

func (s *String) Encode() ([]byte, error) {
	return []byte(*s), nil
}
func (s *String) Decode(b []byte) error {
	*s = String(b)
	return nil
}

func (a *Strings) Encode() ([]byte, error) {
	var buf bytes.Buffer
	intbuffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(intbuffer, uint64(len(*a)))
	buf.Write(intbuffer)
	for _, stringValue := range *a {
		binary.LittleEndian.PutUint64(intbuffer, uint64(len(stringValue)))
		buf.Write(intbuffer)
		buf.Write([]byte(stringValue))
	}
	return buf.Bytes(), nil
}

func (a *Strings) Decode(b []byte) error {
	buf := bytes.NewBuffer(b)
	intbuffer := make([]byte, 8)
	_, err := buf.Read(intbuffer)
	if err != nil {
		return err
	}
	length := binary.LittleEndian.Uint64(intbuffer)
	*a = make([]string, length)
	for i := 0; i < int(length); i++ {
		_, err := buf.Read(intbuffer)
		if err != nil {
			return err
		}
		stringLength := binary.LittleEndian.Uint64(intbuffer)
		stringBuffer := make([]byte, stringLength)
		_, err = buf.Read(stringBuffer)
		if err != nil {
			return err
		}
		(*a)[i] = string(stringBuffer)
	}
	return nil
}
