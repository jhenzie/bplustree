package bplustree

import (
	"errors"
	"time"
)

var (
	ErrNotFound       = errors.New("Not found")
	ErrNotImplemented = errors.New("Not implemented")
)

const (
	OrderedAscending  = -1
	OrderedSame       = 0
	OrderedDescending = 1

	MIN_DEGREE = 3
)

type BTreeKeyGenerator func(tree BTree, value interface{}) (BTreeKey, error)
type BTreeKeyCompare func(lhs, rhs BTreeKey) int

type BTreeKey interface{}

type BTree interface {
	NodeCount() uint64
	Insert(value interface{}) (BTreeKey, error)
	Update(key BTreeKey, value interface{}) error
	Delete(key BTreeKey) error
	Search(key BTreeKey) (interface{}, error)
}

func NewBTree(degree uint16, keyGenerator BTreeKeyGenerator, keyCompare BTreeKeyCompare) BTree {
	tree := new(tree)

	if degree < MIN_DEGREE {
		degree = MIN_DEGREE
	}

	tree.degree = degree
	tree.root = tree.newTreeNode(true)
	tree.keyCompare = keyCompare
	tree.keyGenerator = keyGenerator
	tree.commandQueue = make(chan func())
	tree.stop = make(chan struct{})

	go tree.processCommands()

	return tree
}

func (t *tree) Insert(value interface{}) (BTreeKey, error) {
	ch := make(chan *bTreeTriple)

	t.commandQueue <- func() {
		t.insert(value, ch)
	}

	triple := <-ch

	return triple.key, triple.err
}

func (t *tree) Update(key BTreeKey, value interface{}) error {
	ch := make(chan *bTreeTriple)

	t.commandQueue <- func() {
		t.update(key, value, ch)
	}

	triple := <-ch

	return triple.err
}

func (t *tree) Delete(key BTreeKey) error {
	ch := make(chan *bTreeTriple)

	t.commandQueue <- func() {
		t.delete(key, ch)
	}

	triple := <-ch

	return triple.err
}

func (t *tree) Search(key BTreeKey) (interface{}, error) {
	ch := make(chan *bTreeTriple)

	t.commandQueue <- func() {
		t.search(key, ch)
	}

	triple := <-ch

	return triple.value, triple.err
}

func (t *tree) NodeCount() uint64 {
	return t.nodeCount
}

type tree struct {
	nodeCount    uint64
	degree       uint16
	root         *treeNode
	keyCompare   BTreeKeyCompare
	keyGenerator BTreeKeyGenerator
	dirty        bool
	commandQueue chan func()
	stop         chan struct{}
}

// For leaf nodes, key[idx] -> value[idx]
// For internal nodes children[idx] is a node that has keys less than key[idx]
// For internal nodes, note that len(children) > len(keys)
type treeNode struct {
	internalID uint64
	dirty      bool
	leaf       bool
	keys       []BTreeKey
	values     []interface{}
	children   []*treeNode
	parent     *treeNode
	previous   *treeNode
	next       *treeNode
}

type bTreeTriple struct {
	key   BTreeKey
	value interface{}
	err   error
}

func (t *tree) processCommands() {
	timer := time.Tick(time.Second * 5)
	running := true
	for running {
		select {
		case <-t.stop:
			running = false
		case command := <-t.commandQueue:
			command()
		case <-timer:
			t.houseKeeping()
		}
	}

	t.houseKeeping()
}

func (t *tree) insert(value interface{}, channel chan *bTreeTriple) {
	triple := new(bTreeTriple)

	if key, err := t.keyGenerator(t, value); err != nil {
		triple.err = err
	} else {
		n := t.findNodeForKey(key)
		t.recordValue(key, value, n)
		triple.key = key
	}

	channel <- triple
}

func (t *tree) update(key BTreeKey, value interface{}, channel chan *bTreeTriple) {
	channel <- &bTreeTriple{
		err: ErrNotImplemented,
	}
}

func (t *tree) delete(key BTreeKey, channel chan *bTreeTriple) {
	channel <- &bTreeTriple{
		err: ErrNotImplemented,
	}
}

func (t *tree) search(key BTreeKey, channel chan *bTreeTriple) {
	triple := new(bTreeTriple)
	node := t.findNodeForKey(key)

	triple.err = ErrNotFound

	for idx, k := range node.keys {
		if t.keyCompare(key, k) == OrderedSame {
			triple.value = node.values[idx]
			triple.err = nil
			break
		}
	}

	channel <- triple
}

func (t *tree) findNodeForKey(key BTreeKey) *treeNode {
	n := t.root

	for {
		if n.leaf {
			return n
		}

		var candidateNode *treeNode

		for idx, k := range n.keys {
			if t.keyCompare(key, k) == OrderedAscending {
				candidateNode = n.children[idx]
				break
			}

			candidateNode = n.children[idx+1]
		}

		n = candidateNode
	}
}

func (t *tree) recordValue(key BTreeKey, value interface{}, n *treeNode) {
	insert := -1

	for idx, k := range n.keys {
		if t.keyCompare(key, k) == OrderedAscending {
			insert = idx
			break
		}
	}

	if insert == -1 {
		n.keys = append(n.keys, key)
	} else {
		n.keys = append(n.keys, nil)
		copy(n.keys[insert+1:], n.keys[insert:])
		n.keys[insert] = key
	}

	if n.leaf == false {
		child := value.(*treeNode)
		if insert == -1 {
			n.children = append(n.children, child)
		} else {
			n.children = append(n.children, nil)
			copy(n.children[insert+1:], n.children[insert:])
			n.children[insert] = child
		}
	} else {

		if insert == -1 {
			n.values = append(n.values, value)
		} else {
			n.values = append(n.values, nil)
			copy(n.values[insert+1:], n.values[insert:])
			n.values[insert] = value
		}
	}

	t.splitNode(n)
}

func (t *tree) splitNode(n *treeNode) {
	degree := t.degree

	if n.leaf {
		degree -= 1
	}

	if uint16(len(n.keys)) <= degree {
		return
	}

	splitPoint := t.degree / 2

	if !n.leaf {
		splitPoint += t.degree % 2
	}

	sibling := t.newTreeNode(n.leaf)
	sibling.keys = n.keys[splitPoint:]
	n.keys = n.keys[:splitPoint]

	if n.leaf {
		sibling.values = n.values[splitPoint:]
		n.values = n.values[:splitPoint]
		sibling.previous = n
		n.next = sibling
	} else {
		sibling.children = n.children[splitPoint:]
		n.children = n.children[:splitPoint]
	}

	if n.parent == nil {
		root := t.newTreeNode(false)
		t.root = root
		root.keys = append(root.keys, sibling.keys[0])
		root.children = append(root.children, n, sibling)
		n.parent = root
		sibling.parent = root
	} else {
		sibling.parent = n.parent
		t.recordValue(sibling.keys[0], sibling, n.parent)
	}
}

func (t *tree) newTreeNode(leaf bool) *treeNode {
	n := new(treeNode)

	t.nodeCount += 1

	n.keys = make([]BTreeKey, 0)
	if leaf {
		n.values = make([]interface{}, 0)
	} else {
		n.children = make([]*treeNode, 0)
	}
	n.internalID = t.nodeCount
	n.leaf = leaf
	n.dirty = true
	t.dirty = true

	return n
}

func (t *tree) houseKeeping() {
}
