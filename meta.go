package main

import (
	"encoding/binary"
	"fmt"
)

// Meta page for Persistance
type Meta struct {
	freelistPage PageNum
}

func NewEmptyMeta() *Meta {
	return &Meta{}
}

// Write Meta to Page
func (dal *DAL) WriteMeta(meta *Meta) (*Page, error) {
	page, _ := dal.AllocateEmptyPage()
	page.num = metaPageNum
	meta.serialize(page.data)

	err := dal.WritePage(page)
	if err != nil {
		return nil, err
	}
	return page, nil
}

// Read Meta from Page
func (dal *DAL) ReadMeta() (*Meta, error) {
	page, err := dal.ReadPage(metaPageNum)
	if err != nil {
		return nil, fmt.Errorf("Error reading meta page: %v", err)
	}

	meta := NewEmptyMeta()
	meta.deserialize(page.data)
	return meta, nil
}

// Serialize Data
func (m *Meta) serialize(buf []byte) {
	pos := 0
	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freelistPage))
	pos += pageNumSize
}

// Deserialize Data
func (m *Meta) deserialize(buf []byte) {
	pos := 0
	m.freelistPage = PageNum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize
}
