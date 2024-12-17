package main

import (
	"errors"
	"fmt"
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
func NewDAL(path string, pageSize int32) (*DAL, error) {
	dal := &DAL{
		Meta:     NewEmptyMeta(),
		pageSize: pageSize,
	}

	if _, err := os.Stat(path); err == nil {
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

	} else if errors.Is(err, os.ErrNotExist) {

		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}

		dal.FreeList = NewFreeList()
		dal.freelistPage = dal.GetNextPage()
		_, err := dal.WriteFreeList()
		if err != nil {
			return nil, err
		}
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
	offset := int64(pageNum) * int64(dal.pageSize)
	// Read the page data
	_, err := dal.file.ReadAt(page.data, offset)
	if err != nil {
		return nil, fmt.Errorf("Error reading page: %v", err)
	}
	return page, nil
}

// Write a page to the DAL
func (dal *DAL) WritePage(page *Page) error {
	offset := int64(page.num) * int64(dal.pageSize)
	_, err := dal.file.WriteAt(page.data, offset)
	if err != nil {
		return fmt.Errorf("Error writing page: %v", err)
	}
	return nil
}

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
