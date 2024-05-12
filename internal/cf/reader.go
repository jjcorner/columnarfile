package cf

import "os"

type Reader struct {
}

func NewReader(name string) (*CFile, error) {
	bytes, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}
	reader := &CFile{
		Name:   name,
		fields: make(map[string]*BufferFile),
	}
	reader.Read(bytes)
	return reader, nil
}
