package main

import (
	"bytes"
	"log"
)

// iterates over all items in current node to find key if exists, else return the idx where key should be
func (node *Node) findKeyInNode(key []byte) (bool, int) {
	for i, existingItem := range node.items {
		res := bytes.Compare(existingItem.key, key)
		if res == 0 { // If Match
			return true, i
		}

		// The key is bigger than the previous key, so it doesn't exist in the node, but may exist in child nodes.
		if res == 1 {
			return false, i
		}
	}

	// The key isn't bigger than any of the keys which means it's in the last index
	return false, len(node.items)
}

// searches for key inside the tree, if found returns the index and parent node
func (node *Node) findKey(key []byte) (int, *Node, error) {
	index, n, err := findKeyHelper(node, key)
	if err != nil {
		return -1, nil, err
	}
	return index, n, nil
}

// recursive function, DFS
func findKeyHelper(node *Node, key []byte) (int, *Node, error) {
	// search for key inside the node
	wasFound, idx := node.findKeyInNode(key)
	if wasFound {
		return idx, node, nil
	}

	// leaf node and still key not found -> it doesn't exist
	if node.isLeaf() {
		return -1, nil, nil
	}

	// else search the children
	nextChild, err := node.GetNode(node.childNodes[idx])
	if err != nil {
		return -1, nil, err
	}
	log.Printf("nxtChild: %v, err: %v", nextChild, err)
	// recursive call
	return findKeyHelper(nextChild, key)
}
