package main

import (
	"errors"
	"fmt"
	"log"
	"os"
)

type PageNum uint64

// Page object for OmegaDB
type Page struct {
	num  PageNum
	data []byte
}

// Data Access Layer (DAL) Object for OmegaDB
type DAL struct {
	file     *os.File
	pageSize int32
	// Track allocated and free pages
	*FreeList
	// Meta page
	*Meta
}

// Create a new DAL object
func NewDAL(path string) (*DAL, error) {
	dal := &DAL{
		Meta:     NewEmptyMeta(),
		pageSize: int32(os.Getpagesize()),
	}

	if _, err := os.Stat(path); err == nil { // if file already exists
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}

		meta, err := dal.ReadMeta()
		if err != nil {
			return nil, err
		}
		dal.Meta = meta

		freelist, err := dal.ReadFreelist()
		if err != nil {
			return nil, err
		}
		dal.FreeList = freelist
	} else if errors.Is(err, os.ErrNotExist) { // if file does not exist, create a new file
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}

		// Init freeList
		dal.FreeList = NewFreeList()
		dal.freelistPage = dal.GetNextPage()
		_, err := dal.WriteFreeList()
		if err != nil {
			return nil, err
		}

		// Write Meta page
		_, err = dal.WriteMeta(dal.Meta)
	} else {
		return nil, err
	}
	return dal, nil
}

// Close the DAL
func (dal *DAL) Close() error {
	if dal.file != nil {
		err := dal.file.Close()
		if err != nil {
			return fmt.Errorf("Error closing file: %v", err)
		}
		dal.file = nil
	}
	return nil
}

// Allocate a new page in the DAL
func (dal *DAL) AllocateEmptyPage() (*Page, error) {
	return &Page{
		data: make([]byte, dal.pageSize),
	}, nil
}

// Read a page from the DAL
func (dal *DAL) ReadPage(pageNum PageNum) (*Page, error) {
	page, _ := dal.AllocateEmptyPage()

	// Calculate the offset
	offset := int(pageNum) * int(dal.pageSize)
	// Read the page data
	_, err := dal.file.ReadAt(page.data, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("Error reading page: %v", err)
	}
	return page, nil
}

// Write a page to the file
func (dal *DAL) WritePage(page *Page) error {
	offset := int64(page.num) * int64(dal.pageSize)
	_, err := dal.file.WriteAt(page.data, offset)
	if err != nil {
		return fmt.Errorf("Error writing page: %v", err)
	}
	return nil
}

// Write FreeList into a Page
func (dal *DAL) WriteFreeList() (*Page, error) {
	page, _ := dal.AllocateEmptyPage()
	page.num = dal.freelistPage
	dal.FreeList.serialize(page.data)

	err := dal.WritePage(page)
	if err != nil {
		return nil, err
	}

	dal.freelistPage = page.num
	return page, nil
}

// Read Freelist from a Page
func (dal *DAL) ReadFreelist() (*FreeList, error) {
	page, err := dal.ReadPage(dal.freelistPage)
	if err != nil {
		return nil, err
	}

	freelist := NewFreeList()
	freelist.deserialize(page.data)
	return freelist, nil
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

// Get Node from the DAL
func (dal *DAL) getNode(pageNum PageNum) (*Node, error) {
	page, err := dal.ReadPage(pageNum)
	if err != nil {
		return nil, err
	}

	node := NewEmptyNode()
	log.Println("Debug: ", node)
	node.Deserialize(page.data)
	node.pageNum = pageNum
	return node, nil
}

// Write Node
func (dal *DAL) writeNode(node *Node) (*Node, error) {
	page, _ := dal.AllocateEmptyPage()
	if node.pageNum == 0 {
		page.num = dal.GetNextPage()
		node.pageNum = page.num
	} else {
		page.num = node.pageNum
	}

	page.data = node.Serialize(page.data)

	err := dal.WritePage(page)
	if err != nil {
		return nil, err
	}

	return node, nil
}
