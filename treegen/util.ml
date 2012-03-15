external identity : 'a -> 'a = "%identity"
let (|>) f x = x f
let quad1 (x, _, _, _) = x
let quad2 (_, x, _, _) = x
let quad3 (_, _, x, _) = x
let quad4 (_, _, _, x) = x


module List = struct
  include List

  let init f n =
    let rec init_aux n accum =
      if n <= 0 then accum else
        init_aux (n - 1) (f (n - 1) :: accum)
    in
    init_aux n []

  let sub start len xs =
    try
      let rec skip i xs =
      if i = start then xs
      else skip (i + 1) (List.tl xs)
      and drop j acc xs =
        if j = len then List.rev acc
        else drop (j + 1) (List.hd xs :: acc) (List.tl xs)
      in
      skip 0 xs |> drop 0 []
    with Failure "tl" -> invalid_arg "sub"

  let mapi f =
    let rec mapi_aux f accum n = function
        [] -> List.rev accum
      | x :: xs -> mapi_aux f (f n x :: accum) (n + 1) xs
    in
    mapi_aux f [] 0

  let range a b inc =
    let rec aux a b inc accum =
      if a > b then accum
      else aux a (b - inc) inc (b :: accum)
    in
    if inc = 0 then raise (Invalid_argument "range : increment must be positive.")
    else if inc > 0 && a <= b then aux a (b - (b - a) mod inc) inc []
    else if inc < 0 && a >= b then aux b a (-inc) [] |> List.rev
    else []

  let rec split_at n xs =
    let rec aux n xs acc =
      if n <= 0 || xs = [] then List.rev acc, xs
      else aux (n - 1) (List.tl xs) (List.hd xs :: acc)
    in
    aux n xs []
end

module Stack2 : sig
  type 'a t
  val empty : 'a t
  val singleton : 'a -> 'a t
  val is_empty : 'a t -> bool
  val length : 'a t -> int
  val push : 'a -> 'a t -> 'a t
  val pop : 'a t -> 'a * 'a t
  val remove : 'a t -> 'a t
  val peek : 'a t -> 'a
end = struct
  type 'a t = 'a list
  exception Empty
  let empty = []
  let singleton x = [x]
  let is_empty = function [] -> true | _ -> false
  let length = List.length
  let push x q = x :: q
  let pop = function
      [] -> raise Empty
    | hd :: tl -> (hd, tl)
  let remove = function
      [] -> raise Empty
    | hd :: tl -> tl
  let peek = function
      [] -> raise Empty
    | hd :: tl -> hd
end
