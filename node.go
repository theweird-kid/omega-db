package main

import "encoding/binary"

// B Tree Implementation

type Item struct {
	key   []byte
	value []byte
}

type Node struct {
	// embedded DAL
	*DAL

	// Node properties
	pageNum    PageNum   // Page number of the node
	items      []*Item   // Items in the node
	childNodes []PageNum // Child nodes
}

// Create a new Node
func NewEmptyNode() *Node {
	return &Node{}
}

// Create a new Item
func NewItem(key []byte, value []byte) *Item {
	return &Item{
		key:   key,
		value: value,
	}
}

// check if node is a leaf node
func (n *Node) isLeaf() bool {
	return len(n.childNodes) == 0
}

func (n *Node) Serialize(buf []byte) []byte {
	leftPos := 0
	rightPos := len(buf) - 1

	// Page Headers: isLeaf, key-value pairs count, node number
	// isLeaf
	isleaf := n.isLeaf()
	var bitSetVar uint64
	if isleaf {
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos += 1

	// key-value pairs count
	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(n.items)))
	leftPos += 2

	for i := 0; i < len(n.items); i++ {
		item := n.items[i]
		if !isleaf {
			childnode := n.childNodes[i]

			// Write child page as a fixed size of 8 bytes
			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childnode))
			leftPos += pageNumSize
		}

		klen := len(item.key)
		vlen := len(item.value)

		// Write offset
		offset := rightPos - klen - vlen - 2
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(offset))
		leftPos += 2

		rightPos -= vlen
		copy(buf[rightPos:], item.value)

		rightPos -= 1
		buf[rightPos] = byte(vlen)

		rightPos -= klen
		copy(buf[rightPos:], item.key)

		rightPos -= 1
		buf[rightPos] = byte(klen)
	}

	if !isleaf {
		// Write the last child Node
		lastChildNode := n.childNodes[len(n.childNodes)-1]
		// Write the child Page as a fixed size of 8 bytes
		binary.LittleEndian.PutUint64(buf[leftPos:], uint64(lastChildNode))
	}

	return buf
}

func (n *Node) Deserialize(buf []byte) {
	leftPos := 0

	// Read header
	isleaf := uint16(buf[0])

	itemsCount := int(binary.LittleEndian.Uint16(buf[1:3]))
	leftPos += 3

	// Read Body
	for i := 0; i < itemsCount; i++ {
		// Not a leaf Node
		if isleaf == 0 {
			pageNum := binary.LittleEndian.Uint64(buf[leftPos:])
			leftPos += pageNumSize

			n.childNodes = append(n.childNodes, PageNum(pageNum))
		}

		// Read Offset
		offset := binary.LittleEndian.Uint16(buf[leftPos:])
		leftPos += 2

		// get key len
		klen := uint16(buf[int(offset)])
		offset += 1

		// get key
		key := buf[offset : offset+klen]
		offset += klen

		// get value len
		vlen := uint16(buf[int(offset)])
		offset += 1

		// get value
		value := buf[offset : offset+vlen]
		offset += vlen

		// append item
		n.items = append(n.items, NewItem(key, value))
	}

	// Not a leaf node
	if isleaf == 0 {
		// Read the last child Node
		pageNum := PageNum(binary.LittleEndian.Uint64(buf[leftPos:]))
		n.childNodes = append(n.childNodes, pageNum)
	}
}
