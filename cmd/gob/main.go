package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type Abc struct {
	A int
	B string
	C float64
}

func main() {
	obj := Abc{
		A: 1,
		B: "hello",
		C: 3.14,
	}

	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(obj)
	if err != nil {
		panic(err)
	}

	fmt.Println(obj.A)
	fmt.Println(obj.B)
	fmt.Println(obj.C)

	fmt.Println(buf.Bytes())
}
