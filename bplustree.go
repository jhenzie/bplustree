package bplustree

import (
	"errors"
	"log"
)

// B+Tree stores value at the leaf only, internal nodes only contain copies of the keys and pointers child nodes

type Tree interface {
	Delete(key Key) error
	Insert(val interface{}) (Key, error)
	NodeCount() uint64
	Search(key Key) (interface{}, error)
	Update(key Key, val interface{}) error
}

type Key interface{}

type KeyCompareFn func(lhs, rhs Key) int

type KeyGenerationFn func(t Tree, value interface{}) Key

type tree struct {
	degree       uint16
	dirty        bool
	firstKey     Key
	keyGenerator KeyGenerationFn
	keyCompare   KeyCompareFn
	nodeCount    uint64
	root         *treeNode
}

type treeNode struct {
	children []*treeNode
	dirty    bool
	keys     []Key
	leaf     bool
	next     *treeNode
	parent   *treeNode
	previous *treeNode
	values   []interface{}
}

const (
	OrderedAscending  = -1
	OrderedSame       = 0
	OrderedDescending = 1
)

var (
	ErrInternalInconsistency = errors.New("Tree is internally inconsistent")
	ErrNotFound              = errors.New("Provided key not found")
	ErrNotImplemented        = errors.New("Not implemented, sorry")
)

func NewTree(degree uint16, keyGenerator KeyGenerationFn, keyCompare KeyCompareFn) Tree {

	if degree < 2 {
		degree = 2
	}

	t := new(tree)
	t.root = t.newTreeNode(true)
	t.degree = degree
	t.keyCompare = keyCompare

	// one time function to capture the first key and then
	// revert to normal key generation
	keyGenFunc := func(tree Tree, value interface{}) Key {
		t.firstKey = keyGenerator(t, value)
		t.keyGenerator = keyGenerator
		return t.firstKey
	}

	t.keyGenerator = keyGenFunc

	return t
}

// Implement Tree interface on Tree

func (t *tree) NodeCount() uint64 {
	return t.nodeCount
}

func (t *tree) Insert(val interface{}) (Key, error) {
	key := t.keyGenerator(t, val)
	node := t.nodeForKey(key)
	err := t.recordValue(key, val, node)

	return key, err
}

func (t *tree) Delete(key Key) error {
	// TODO
	return ErrNotImplemented
}

func (t *tree) Update(key Key, val interface{}) error {
	return ErrNotImplemented
}

func (t *tree) Search(key Key) (interface{}, error) {
	n := t.nodeForKey(key)

	for idx, k := range n.keys {
		if t.keyCompare(key, k) == OrderedSame {
			return n.values[idx], nil
		}
	}

	return nil, ErrNotFound
}

// Internal funcs

func (t *tree) newTreeNode(isLeaf bool) *treeNode {
	nnode := new(treeNode)
	nnode.leaf = isLeaf
	nnode.dirty = true
	nnode.children = make([]*treeNode, 0, t.degree+1)
	nnode.keys = make([]Key, 0, t.degree)
	nnode.values = make([]interface{}, 0, t.degree)
	t.dirty = true
	t.nodeCount += 1

	return nnode
}

func (t *tree) recordValue(key Key, value interface{}, n *treeNode) error {
	n, err := t.splitNode(n, key)

	if err != nil {
		return err
	}

	place := -1

	for idx, nkey := range n.keys {
		if t.keyCompare(key, nkey) == OrderedAscending {
			place = idx
			break
		}
	}

	if place == -1 {
		n.keys = append(n.keys, key)
		if n.leaf {
			n.values = append(n.values, value)
			return nil
		} else {
			if tn, ok := value.(*treeNode); ok {
				n.children = append(n.children, tn)
				return nil
			} else {
				return ErrInternalInconsistency
			}
		}
	} else {
		keyCount := len(n.keys)
		n.keys = n.keys[0 : keyCount+1]
		copy(n.keys[place+1:], n.keys[place:])
		n.keys[place] = key
		if n.leaf {
			n.values = n.values[0 : keyCount+1]
			copy(n.values[place+1:], n.values[place:])
			n.values[place] = value
			return nil
		} else {
			if tn, ok := value.(*treeNode); ok {
				childCount := len(n.children)
				n.children = n.children[0 : childCount+1]
				copy(n.children[place+1:], n.children[place:])
				n.children[place] = tn
				return nil
			} else {
				return ErrInternalInconsistency
			}
		}
	}
}

func (t *tree) splitNode(n *treeNode, key Key) (*treeNode, error) {
	keyCount := len(n.keys)

	if uint16(keyCount) < t.degree {
		return n, nil
	}

	n.dirty = true

	split := keyCount / 2
	newNode := t.newTreeNode(true)
	newNode.keys = n.keys[split:]
	if newNode.leaf {
		newNode.previous = n
		n.next = newNode
		newNode.values = n.values[split:]
		n.values = n.values[:split]
	} else {
		newNode.children = n.children[split:]
		n.children = n.children[:split]
	}

	if n.parent == nil {
		root := t.newTreeNode(false)
		root.leaf = false
		t.root = root

		root.keys = append(root.keys, newNode.keys[0])
		root.children = append(root.children, n, newNode)
	} else {
		// Bubble up the new node to parent
		if err := t.recordValue(newNode.keys[0], newNode, n.parent); err != nil {
			return nil, err
		}
	}

	if t.keyCompare(key, newNode.keys[0]) == OrderedAscending {
		return n, nil
	} else {
		return newNode, nil
	}
}

func (t *tree) nodeForKey(k Key) *treeNode {
	candidateNode := t.root

	for {
		if candidateNode.leaf {
			return candidateNode
		}

		for idx, key := range candidateNode.keys {
			switch t.keyCompare(k, key) {
			case OrderedSame:
				candidateNode = candidateNode.children[idx+1]
				break
			case OrderedAscending:
				candidateNode = candidateNode.children[idx]
				break
			case OrderedDescending:
				candidateNode = candidateNode.children[idx+1]
				continue
			}
		}
	}
}

func (t *tree) houseKeeping() {
	log.Printf("Performing houseKeeping")
}
