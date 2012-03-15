type 'a tree = Leaf of 'a | Node of 'a * ('a tree list)
type 'a serialized = (int * 'a option) list
