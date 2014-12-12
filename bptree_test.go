package bplustree

import "testing"

type kt uint64

func uint64Key() KeyGenFn {
	var currentValue kt = 1000
	return func(t Tree, val interface{}) Key {
		currentValue += 1
		return currentValue

	}
}

func (k kt) Compare(other Key) int {
	key2 := other.(kt)

	if k < key2 {
		return LessThan
	} else if k > key2 {
		return GreaterThan
	}

	return Equal
}

func TestInsert(test *testing.T) {
	fn := uint64Key()
	tree := NewTree(2, 10, fn)

	if tree.NodeCount() != 1 {
		test.Error("New tree should have 1 nodes, has", tree.NodeCount())
	}

	ch := tree.Insert("Justin")

	// wait for the result
	tor := <-ch

	if tor.Err != nil {
		test.Error("Insertion results in error", tor.Err)
	}

	test.Log("Insert generated key", tor.Key)

	if tree.NodeCount() != uint64(1) {
		test.Error("Expected 1 got", tree.NodeCount())
	}

	ch = tree.Insert("Nicky")

	tor = <-ch

	if tree.NodeCount() != uint64(1) {
		test.Error("Expected 1 got", tree.NodeCount())
	}

	// This insert will exceed the count for this node, causing a split
	// and the creation of a new root.
	ch = tree.Insert("Caitlin")

	tor = <-ch

	if tree.NodeCount() != uint64(3) {
		test.Error("Expected 3 got", tree.NodeCount())
	}

}
