package bplustree

import "testing"

func populateTree(t BTree, test *testing.T) {
	values := []string{"justin", "nicky", "caitlin", "abigail", "jasper"}

	for _, val := range values {
		if _, err := t.Insert(val); err != nil {
			test.Error("Error inserting into tree")
		}
	}
}

func initTree(test *testing.T) BTree {
	var seed uint64 = 100

	key := func(t BTree, v interface{}) (BTreeKey, error) {
		seed += 1
		return seed, nil
	}

	keyCompare := func(lhs, rhs BTreeKey) int {
		lhss := lhs.(uint64)
		rhss := rhs.(uint64)

		if lhss < rhss {
			return OrderedAscending
		} else if lhss > rhss {
			return OrderedDescending
		}

		return OrderedSame
	}

	t := NewBTree(4, key, keyCompare)

	return t
}

func TestInsert(test *testing.T) {
	t := initTree(test)

	populateTree(t, test)

	// 5 entries and a degree of 4 should yield 3 nodes
	// 1 -> Just a leaf node with 1 key
	// 2 -> Just a leaf node with 2 keys
	// 3 -> Just a leaf node with 3 keys
	// 4 -> Two leaf nodes with 2 keys each and a root node with 1 key
	// 5 -> Two lead nodes with 2 and 3 keys respectively and a root node with 1 key
	if t.NodeCount() != 3 {
		test.Error("Node count should be 3 after 5 insertions, is ", t.NodeCount())
	}
}

func TestSearch(test *testing.T) {
	t := initTree(test)
	testValue := "TestSearchValue"
	if key, err := t.Insert(testValue); err != nil {
		test.Error("Insert failed with error:", err)
	} else {
		// put more in the tree
		populateTree(t, test)

		if value, err := t.Search(key); err != nil {
			test.Error("Search failed with error:", err)
		} else if value != testValue {
			test.Error("Expecting value ", testValue, ", received ", value)
		}
	}

}
