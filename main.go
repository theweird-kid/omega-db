package main

import (
	"fmt"
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
	//_ = dal.WritePage(page)
	data, _ := dal.ReadPage(initialPageNum)
	fmt.Println(string(data.data))
}
