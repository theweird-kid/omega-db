package main

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
