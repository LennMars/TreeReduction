open Tree

val to_list : 'a tree -> 'a serialized
val to_string : ('a -> string) -> 'a serialized -> string
val of_list : 'a serialized -> 'a tree
val of_list2 : 'a serialized -> 'a tree
val to_split_string : ('a -> string) -> int -> 'a serialized -> string
