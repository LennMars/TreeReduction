open Util
open Tree

let tree =
  Node (3,
  [Leaf (4);
  Node ((-5),
  [Node (6,
  [Leaf ((-2)); Leaf (8); Node ((-1), [Leaf (4)])]);
  Leaf (1)]);
  Leaf (5); Node (2, [Leaf ((-6))])])

let tree_ = Node(1, [Leaf(2); Node(3, [Leaf 4; Leaf 5]); Leaf 6])

let top = function
    Leaf x -> x
  | Node (x, ts) -> x

let rec traverse f = function
    Leaf x -> f x
  | Node (x, ts) -> (f x; List.iter (traverse f) ts)

let size tree =
  let ans = ref 0 in
  traverse (fun _ -> incr ans) tree;
  !ans

let to_list_deprecated tree =
  let rec aux depth = function
      Leaf x -> [(depth, Some x); (depth, None)]
    | Node (x, ts) -> (depth, Some x) :: (List.concat (List.map (aux (succ depth)) ts)) @ [(depth, None)]
  in
  aux 0 tree

let to_list tree =
  let len = size tree * 2 in
  let xs = Array.make len (-1, None) in
  let rec aux depth l = function
      Leaf x ->
        xs.(l) <- depth, Some x;
        xs.(l + 1) <- depth, None;
        l + 2
    | Node (x, ts) ->
        xs.(l) <- depth, Some x;
        let f l t = aux (depth + 1) l t in
        let l = List.fold_left f (l + 1) ts in
        xs.(l) <- depth, None;
        l + 1
  in
  aux 0 0 tree |> ignore;
  xs |> Array.to_list

let unsome = function
    Some x -> x
  | None -> invalid_arg "unsome"

let child list =
  match list with
    [] -> invalid_arg "child"
  | (hd_depth, hd_x) :: tl ->
      let rec aux acc = function
          [] -> invalid_arg "child"
        | l :: r ->
            if fst l = hd_depth then (unsome hd_x, List.rev acc), r
            else aux (l :: acc) r
      in
      aux [] tl

let children list =
  let rec aux acc = function
    [] -> List.rev acc
  | list ->
      let c, rest = child list in
      aux (c :: acc) rest
  in
  aux [] list

let of_list list =
  let rec aux (x, rest) =
    match rest with
      [] -> Leaf x
    | _ -> Node (x, List.map aux (children rest))
  in
  aux (children list |> List.hd)

let of_list2 list =
  let open List in
  let rec aux depth opens usubs bsubs = function
    | [] -> hd bsubs
    | (d, x) :: r -> match x with
        | Some x when d = depth -> aux d (x :: opens) ((d, bsubs) :: usubs) [] r
        | Some x -> aux d (x :: opens) usubs bsubs r
        | None ->
            let t = match bsubs with
              | [] -> Leaf(hd opens)
              | _ -> Node(hd opens, rev bsubs)
            in
            match usubs with
            | (d_, ts) :: tr when d_ = d ->
                aux d (tl opens) tr (t :: ts) r
            | _ ->
                aux d (tl opens) usubs [t] r

  in
  aux (-1) [] [] [] list

let to_string elm_to_string list = List.map (fun (d, x) -> string_of_int d ^ " " ^ match x with Some x -> elm_to_string x | None -> "/") list |>
    String.concat " "

let to_split_string elm_to_string n list =
  let m = List.length list / n |> max 1 in
  let rec aux acc = function
      [] -> List.rev acc
    | rest ->
        let left, rest = List.split_at m rest in
        aux (left :: acc) rest
  in
  aux [] list |> List.mapi (fun i l -> string_of_int i ^ "\t" ^ to_string elm_to_string l) |> String.concat "\n"
