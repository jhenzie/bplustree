# B+Tree

All values are kept in leaf nodes, with internal nodes continaing only keys.

## Search strategy
Currently if the degree is < 20 a simple linear search is used to locate key placement within a nodes keys.  If the degree is  > 20,
binary search is used.

