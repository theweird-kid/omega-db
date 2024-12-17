package main

import (
	"encoding/binary"
)

var initialPageNum = PageNum(1)

// Maintains a list of free pages
type FreeList struct {
	maxPage       PageNum   // Maximum page number
	releasedPages []PageNum // List of released pages
}

// Create a new FreeList object
func NewFreeList() *FreeList {
	return &FreeList{
		maxPage:       initialPageNum,
		releasedPages: []PageNum{},
	}
}

// Get Next page
func (list *FreeList) GetNextPage() PageNum {
	// If there are released pages, return the first one
	if len(list.releasedPages) > 0 {
		page := list.releasedPages[0]
		list.releasedPages = list.releasedPages[1:]
		return page
	}
	// else return a new Page
	list.maxPage += 1
	return list.maxPage
}

// Release a page
func (list *FreeList) ReleasePage(page PageNum) {
	list.releasedPages = append(list.releasedPages, page)
}

// Seralize FreeList
func (list *FreeList) serialize(buf []byte) []byte {
	pos := 0

	// max page
	binary.LittleEndian.PutUint16(buf[pos:], uint16(list.maxPage))
	pos += 2

	// released Page count
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(list.releasedPages)))
	pos += 2

	// released pages
	for _, page := range list.releasedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += pageNumSize
	}

	return buf
}

func (list *FreeList) deserialize(buf []byte) {
	pos := 0

	// max page
	list.maxPage = PageNum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	// released Page count
	releasedPageCount := int32(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	// released pages
	for i := 0; i < int(releasedPageCount); i++ {
		page := PageNum(binary.LittleEndian.Uint64(buf[pos:]))
		list.releasedPages = append(list.releasedPages, page)
		pos += pageNumSize
	}
}
