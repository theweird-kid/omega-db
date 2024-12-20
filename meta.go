package main

import (
	"encoding/binary"
)

// Meta page for Persistance
type Meta struct {
	root         PageNum
	freelistPage PageNum
}

func NewEmptyMeta() *Meta {
	return &Meta{}
}

// Serialize Data
func (m *Meta) serialize(buf []byte) {
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.root))
	pos += pageNumSize

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freelistPage))
	pos += pageNumSize
}

// Deserialize Data
func (m *Meta) deserialize(buf []byte) {
	pos := 0

	m.root = PageNum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize

	m.freelistPage = PageNum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize
}
