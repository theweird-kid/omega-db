package main

import (
	"os"
)

func main() {
	// init omega-db
	dal, _ := NewDAL("omega.db", int32(os.Getpagesize()))

	// create a new page
	page, _ := dal.AllocateEmptyPage()
	page.num = dal.GetNextPage()
	copy(page.data, []byte("Hello, World!"))

	// commit update
	_ = dal.WritePage(page)
	_, _ = dal.WriteFreeList()

	// close omega-db
	_ = dal.Close()

	// re-open omega-db
	dal, _ = NewDAL("omega.db", int32(os.Getpagesize()))

	// create a new page
	page, _ = dal.AllocateEmptyPage()
	page.num = dal.GetNextPage()
	copy(page.data[:], []byte("Bye, World!"))
	_ = dal.WritePage(page)

	// Create a page and release it
	pagenum := dal.GetNextPage()
	dal.ReleasePage(pagenum)

	// commit update
	_, _ = dal.WriteFreeList()
}
