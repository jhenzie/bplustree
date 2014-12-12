package bplustree

import (
	"errors"
	"log"
	"time"
)

// B+Tree stores value at the leaf only, internal nodes only contain copies of the keys and pointers child nodes

type Tree interface {
	Stop()

	Delete(key Key) chan *TreeOpResult
	Insert(val interface{}) chan *TreeOpResult
	NodeCount() uint64
	Search(key Key) chan *TreeOpResult
	Update(key Key, val interface{}) chan *TreeOpResult
}

type Key interface {
	Compare(other Key) int
}

type TreeOpResult struct {
	Key   Key
	Value interface{}
	Err   error
}

type KeyGenFn func(t Tree, value interface{}) Key

type operation func()

type tree struct {
	degree         int
	dirty          bool
	keyGenerator   KeyGenFn
	nodeCount      uint64
	operationQueue chan operation
	root           *treeNode
	stop           chan interface{}
}

type treeNode struct {
	children []*treeNode
	dirty    bool
	keys     []Key
	leaf     bool
	nextLeaf *treeNode
	parent   *treeNode
	values   []interface{}
}

const (
	LessThan    = -1
	Equal       = 0
	GreaterThan = 1
)

var (
	ErrInternalInconsistency = errors.New("Tree is internally inconsistent")
	ErrNotFound              = errors.New("Provided key not found")
)

func NewTree(degree int, queueLength uint16, keyGenerator KeyGenFn) Tree {
	if degree < 2 {
		degree = 2
	}

	if queueLength < 1 {
		queueLength = 1
	}

	t := new(tree)
	t.root = t.newTreeNode()
	t.root.leaf = true
	t.root.dirty = true
	t.dirty = true
	t.degree = degree
	t.operationQueue = make(chan operation, queueLength)
	t.stop = make(chan interface{})
	t.keyGenerator = keyGenerator
	go t.start()

	return t
}

// Implement Tree interface on Tree

func (t *tree) NodeCount() uint64 {
	return t.nodeCount
}

func (t *tree) Insert(val interface{}) chan *TreeOpResult {
	ch := make(chan *TreeOpResult)

	t.operationQueue <- func() {
		t.insert(val, ch)
	}

	return ch
}

func (t *tree) Delete(key Key) chan *TreeOpResult {
	ch := make(chan *TreeOpResult)

	t.operationQueue <- func() {
		t.delete(key, ch)
	}

	return ch
}

func (t *tree) Update(key Key, val interface{}) chan *TreeOpResult {
	ch := make(chan *TreeOpResult)

	t.operationQueue <- func() {
		t.update(key, val, ch)
	}

	return ch
}

func (t *tree) Search(key Key) chan *TreeOpResult {
	ch := make(chan *TreeOpResult)

	t.operationQueue <- func() {
		t.search(key, ch)
	}

	return ch
}

func (t *tree) Stop() {
	t.stop <- true
}

// Internal funcs

func (t *tree) newTreeNode() *treeNode {
	nnode := new(treeNode)
	nnode.dirty = true
	nnode.children = make([]*treeNode, 0, t.degree+1)
	nnode.keys = make([]Key, 0, t.degree)
	nnode.values = make([]interface{}, 0, t.degree)
	t.dirty = true
	t.nodeCount += 1

	log.Printf("Node count is now %v", t.nodeCount)

	return nnode
}

func (t *tree) start() {
	inactivityTimer := time.Tick(time.Second * 3)
	listening := true

	for listening {
		select {
		case <-t.stop:
			listening = false
		case op := <-t.operationQueue:
			op()
		case <-inactivityTimer:
			t.houseKeeping()
		}
	}

	t.houseKeeping()
}

func (t *tree) insert(value interface{}, channel chan *TreeOpResult) {
	key := t.keyGenerator(t, value)
	node := t.nodeForKey(key)

	err := t.recordValue(key, value, node)

	tor := new(TreeOpResult)

	tor.Value = value
	tor.Key = key
	tor.Err = err

	channel <- tor
}

func (t *tree) update(key Key, val interface{}, channel chan *TreeOpResult) {
}

func (t *tree) search(key Key, channel chan *TreeOpResult) {
	tor := new(TreeOpResult)
	node := t.nodeForKey(key)

	if node != nil {
		for idx, nkey := range node.keys {
			if key.Compare(nkey) == Equal {
				tor.Value = node.values[idx]
				tor.Key = key

				channel <- tor
				return
			}
		}
	}

	tor.Err = ErrNotFound
	channel <- tor
}

func (t *tree) delete(key Key, channel chan *TreeOpResult) {
	tor := new(TreeOpResult)
	node := t.nodeForKey(key)

	if node != nil {
		for idx, nkey := range node.keys {
			if key.Compare(nkey) == Equal {
				tor.Value = node.values[idx]
				tor.Key = key

				// Deletion here

				channel <- tor
				return
			}
		}
	}

	tor.Err = ErrNotFound
	channel <- tor
}

func (t *tree) recordValue(key Key, value interface{}, n *treeNode) error {
	n, err := t.splitNode(n, key)

	if err != nil {
		return err
	}

	place := -1

	for idx, nkey := range n.keys {
		if key.Compare(nkey) == LessThan {
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

	if keyCount < t.degree {
		return n, nil
	}

	split := keyCount / 2
	newNode := t.newTreeNode()
	newNode.leaf = n.leaf
	newNode.keys = n.keys[split:]
	if newNode.leaf {
		newNode.values = n.values[split:]
		n.values = n.values[:split]
	} else {
		newNode.children = n.children[split:]
		n.children = n.children[:split]
	}

	if n.parent == nil {
		root := t.newTreeNode()
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

	if key.Compare(newNode.keys[0]) == LessThan {
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
			switch k.Compare(key) {
			case Equal:
				candidateNode = candidateNode.children[idx+1]
				break
			case LessThan:
				candidateNode = candidateNode.children[idx]
				break
			case GreaterThan:
				candidateNode = candidateNode.children[idx+1]
				continue
			}
		}
	}
}

func (t *tree) houseKeeping() {
	log.Printf("Performing houseKeeping")
}
