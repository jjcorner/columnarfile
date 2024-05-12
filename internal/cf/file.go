package cf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strconv"

	"github.com/vkumbhar94/columnarfile/internal/types"
)

type CFile struct {
	Name   string
	fields map[string]*BufferFile
	idx    uint64
}

func (f *CFile) Write(m map[string]types.Typ) {
	idxbuffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(idxbuffer, f.idx)
	for k, v := range m {
		if _, ok := f.fields[k]; !ok {
			f.fields[k] = &BufferFile{
				Name: k,
				Type: v.Type(),
			}
		}

		fmt.Println("writing: ", v.Iface())

		bytes, err := v.Encode()
		intbuffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(intbuffer, uint64(len(bytes)))
		f.fields[k].data = append(f.fields[k].data, intbuffer...)
		f.fields[k].data = append(f.fields[k].data, idxbuffer...)
		if err != nil {
			panic(err)
		}
		f.fields[k].data = append(f.fields[k].data, bytes...)
	}
	f.idx++
}

func (f *CFile) Flush() error {
	file, err := os.OpenFile(f.Name, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)
	bytes, err := f.ToBytes()
	if err != nil {
		return err
	}
	_, err = file.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

const (
	Magic             = uint64(0xCACACACA)
	Version           = uint64(1)
	FileHeaderVersion = uint64(1)
)

type FileHeaderV1 struct {
	FieldName              string
	FieldType, Offset, Len uint64
}

func (v *FileHeaderV1) Encode() []byte {
	buf := make([]byte, 8*4)
	binary.LittleEndian.PutUint64(buf[:8], v.FieldType)
	binary.LittleEndian.PutUint64(buf[8:16], v.Offset)
	binary.LittleEndian.PutUint64(buf[16:24], v.Len)
	fbytes := []byte(v.FieldName)

	binary.LittleEndian.PutUint64(buf[24:32], uint64(len(fbytes)))
	return append(buf, fbytes...)
}
func (v *FileHeaderV1) Decode(buf []byte) {
	v.FieldType = binary.LittleEndian.Uint64(buf[:8])
	v.Offset = binary.LittleEndian.Uint64(buf[8:16])
	v.Len = binary.LittleEndian.Uint64(buf[16:24])
	fieldNameLength := binary.LittleEndian.Uint64(buf[24:32])

	v.FieldName = string(buf[32 : 32+fieldNameLength])
}

func (f *CFile) ToBytes() ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, Magic)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buf, binary.LittleEndian, Version)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buf, binary.LittleEndian, FileHeaderVersion)
	if err != nil {
		return nil, err
	}
	offset := uint64(0)
	var headers []FileHeaderV1
	for _, v := range f.fields {
		fh := FileHeaderV1{
			FieldName: v.Name,
			FieldType: uint64(v.Type),
			Offset:    offset,
			Len:       uint64(len(v.data)),
		}

		headers = append(headers, fh)
		offset += uint64(len(v.data))
	}
	err = binary.Write(&buf, binary.LittleEndian, uint64(len(headers)))
	if err != nil {
		return nil, err
	}
	var hbuf bytes.Buffer
	for _, h := range headers {
		bytes := h.Encode()

		err = binary.Write(&hbuf, binary.LittleEndian, uint64(len(bytes)))
		if err != nil {
			return nil, err
		}
		err = binary.Write(&hbuf, binary.LittleEndian, bytes)
		if err != nil {
			return nil, err
		}
	}
	err = binary.Write(&buf, binary.LittleEndian, uint64(buf.Len()+hbuf.Len()+8))
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buf, binary.LittleEndian, hbuf.Bytes())
	if err != nil {
		return nil, err
	}
	for _, h := range headers {
		buf.Write(f.fields[h.FieldName].data)
	}
	return buf.Bytes(), nil
}

func (f *CFile) Read(file []byte) {
	f.FromBytes(file)
}

func (f *CFile) FromBytes(file []byte) {
	buf := bytes.NewBuffer(file)
	magic := make([]byte, 8)
	_, err := buf.Read(magic)
	if err != nil {
		panic(err)
	}
	if binary.LittleEndian.Uint64(magic) != Magic {
		panic("not a columnar file")
	}
	version := make([]byte, 8)
	_, err = buf.Read(version)
	if err != nil {
		panic(err)
	}
	if binary.LittleEndian.Uint64(version) != Version {
		panic("unsupported version")
	}
	fileHeaderVersion := make([]byte, 8)
	_, err = buf.Read(fileHeaderVersion)
	if err != nil {
		panic(err)
	}
	if binary.LittleEndian.Uint64(fileHeaderVersion) != FileHeaderVersion {
		panic("unsupported file header version")
	}
	headers := make([]FileHeaderV1, 0)
	headerCount := make([]byte, 8)
	_, err = buf.Read(headerCount)
	if err != nil {
		panic(err)
	}
	headerLen := make([]byte, 8)
	_, err = buf.Read(headerLen)
	if err != nil {
		panic(err)
	}

	for i := 0; i < int(binary.LittleEndian.Uint64(headerCount)); i++ {
		hlen := make([]byte, 8)
		_, err = buf.Read(hlen)
		if err != nil {
			panic(err)
		}

		hbuf := make([]byte, binary.LittleEndian.Uint64(hlen))
		_, err = buf.Read(hbuf)
		if err != nil {
			panic(err)
		}
		h := FileHeaderV1{}

		h.Decode(hbuf)
		headers = append(headers, h)
	}
	for _, h := range headers {
		start := h.Offset + binary.LittleEndian.Uint64(headerLen)
		end := start + h.Len
		data := file[start:end]
		f.fields[h.FieldName] = &BufferFile{
			Name: h.FieldName,
			Type: types.DType(h.FieldType),
			data: data,
		}
	}
}

func (f *CFile) Iterator() {
	for _, v := range f.fields {
		switch v.Type {
		case types.IntType:
			buf := bytes.NewBuffer(v.data)
			for {
				intbuffer := make([]byte, 8)
				_, err := buf.Read(intbuffer)
				if err != nil {
					break
				}
				len := binary.LittleEndian.Uint64(intbuffer)
				idxbuffer := make([]byte, 8)
				_, err = buf.Read(idxbuffer)
				if err != nil {
					break
				}
				idx := binary.LittleEndian.Uint64(idxbuffer)
				valbuffer := make([]byte, 8)
				_, err = buf.Read(valbuffer)
				if err != nil {
					break
				}
				_, _ = len, idx
				val := binary.LittleEndian.Uint64(valbuffer)
				println("int: ", val)
			}
		case types.FloatType:
			buf := bytes.NewBuffer(v.data)
			for {
				intbuffer := make([]byte, 8)
				_, err := buf.Read(intbuffer)
				if err != nil {
					break
				}
				len := binary.LittleEndian.Uint64(intbuffer)
				idxbuffer := make([]byte, 8)
				_, err = buf.Read(idxbuffer)
				if err != nil {
					break
				}
				idx := binary.LittleEndian.Uint64(idxbuffer)
				valbuffer := make([]byte, 8)
				_, err = buf.Read(valbuffer)
				if err != nil {
					break
				}
				_, _ = len, idx
				val := binary.LittleEndian.Uint64(valbuffer)
				f := math.Float64frombits(val)
				println("float: ", f, strconv.FormatFloat(f, 'f', -1, 64))

			}
		}
	}
}

type BufferFile struct {
	Name string
	Type types.DType
	data []byte
}

func NewFile(name string) *CFile {
	return &CFile{
		Name:   name,
		fields: make(map[string]*BufferFile),
	}
}
