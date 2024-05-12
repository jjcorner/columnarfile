package main

import (
	"fmt"

	"github.com/vkumbhar94/columnarfile/internal/cf"
	"github.com/vkumbhar94/columnarfile/internal/types"
)

func main() {
	fmt.Println("starting...")

	file := cf.NewFile("abc.xyz")

	file.Write(map[string]types.Typ{
		"intfname":   getIntType(123),
		"floatfname": getFloatType(123.456),
	})

	err := file.Flush()
	if err != nil {
		panic(err)
	}

	reader, err := cf.NewReader("abc.xyz")
	if err != nil {
		panic(err)
	}

	reader.Iterator()

	fmt.Println("done")
}

func getIntType(i int) types.Typ {
	a := types.Int(i)
	return &a
}

func getFloatType(f float64) types.Typ {
	a := types.Float(f)
	return &a
}
