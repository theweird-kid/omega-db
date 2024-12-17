package main

import (
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
}

// Create a new DAL object
func NewDAL(path string, pageSize int32) (*DAL, error) {
	dal := &DAL{}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	dal.file = file
	dal.pageSize = pageSize
	dal.FreeList = NewFreeList()
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
