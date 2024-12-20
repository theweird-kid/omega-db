package main

import (
	"fmt"
)

func main() {
	// init db
	dal, _ := NewDAL("mainTest")

	node, _ := dal.getNode(dal.root)
	node.DAL = dal

	idx, containingNode, _ := node.findKey([]byte("Key1"))
	res := containingNode.items[idx]

	fmt.Printf("key is: %s, value is: %s\n", res.key, res.value)
	// close the db
	_ = dal.Close()
}
