package bplustree

import "testing"

type kt uint64

func uint64Key() KeyGenerationFn {
	var currentValue kt = 1000
	return func(t Tree, val interface{}) Key {
		currentValue += 1
		return currentValue

	}
}

func keyCompare(lhs, rhs Key) int {
	lhsi := lhs.(kt)
	rhsi := rhs.(kt)

	if lhsi < rhsi {
		return OrderedAscending
	} else if lhsi > rhsi {
		return OrderedDescending
	}

	return OrderedSame
}

func initTree() Tree {
	return NewTree(2, uint64Key(), keyCompare)
}

func populateTree(t Tree) {
	t.Insert("justin")
	t.Insert("nicky")
	t.Insert("caitlin")
	t.Insert("abigail")
}

func TestInsert(test *testing.T) {
	tree := initTree()

	populateTree(tree)

	if tree.NodeCount() != 5 {
		test.Error("Tree should have 5 nodes, has ", tree.NodeCount())
	}
}

func TestSearch(test *testing.T) {
	tree := initTree()

	populateTree(tree)

	if key, err := tree.Insert("jasper"); err != nil {
		test.Error("Tree insert failed with error", err)
	} else {
		if name, err := tree.Search(key); err != nil {
			test.Error("Search for key yielded error:", err)
		} else if name != "jasper" {
			test.Error("Retrieved value should have been jasper, was ", name)
		}
	}
}
