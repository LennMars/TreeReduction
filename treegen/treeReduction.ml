open Util
open Tree
open Printf

let debug = ref false

module type VType = sig
  type t1
  type t2
  val h' : t1 -> t2
  val ( *^ ) : t2 -> t2 -> t2
  val ( +^ ) : t1 -> t2 -> t2
  val unit : t2
  val unit_inv : t1
end

module type M = sig
  type t1
  type t2
  type tree = (int * t1 option) list
  type phase1_return = {c : t2 array; ab : (t1 * t2) array; d : int}
  type triplet = Normal of t1 * t2 * t2 | Addition of t2 * (t1 * t2 * t2)
  type mating = int * int * int * int
  val phase1 : int -> int -> tree -> phase1_return
  val phase1_parallel : tree list -> phase1_return array
  val phase2 : phase1_return array -> mating list
  val get_triplet : phase1_return array -> mating -> triplet list
end

module Make(V : VType) = struct
  type t1 = V.t1
  type t2 = V.t2
  type tree = (int * t1 option) list
  type phase1_return = {c : t2 array; ab : (t1 * t2) array; d : int}
  type triplet = Normal of t1 * t2 * t2 | Addition of t2 * (t1 * t2 * t2)
  type mating = int * int * int * int
  let unit = V.unit
  let unit_inv = V.unit_inv
  let ( *^ ) = V.( *^ )
  let ( +^ ) = V.( +^ )
  let is_edge_proc num_proc me_proc = me_proc = 0 || me_proc = num_proc - 1
  let phase1 num_proc me_proc t =
    let open List in
    if t = [] then invalid_arg "phase1"
    else
      let c, a, b = match hd t |> snd with
	  None -> [unit; unit], [], []
        | Some a0 -> [unit], [a0], [unit]
      and d = hd t |> fst
      in
      let rec aux c a b d = function
	  [] ->
            let ab = combine a b |> rev |> Array.of_list in
            let c =
              if num_proc > 1 && is_edge_proc num_proc me_proc then tl c else c
            in
(*             let c, ab, d =
              if num_proc > 1 && not (is_edge_proc num_proc me_proc) && Array.length ab = 0 then (* internal *)
                unit :: c, [|unit_inv, unit|], d - 1
              else
                c, ab, d
            in *)
            {c = Array.of_list c; ab; d}
        | (di, ai) :: ttl ->
	    match ai with
	      None ->
	        (match a with
		  [] ->
		    aux (unit :: c) a b di ttl
	        | ahd :: atl ->
		    let bhd, btl = hd b, tl b in
		    let t = if bhd = unit then V.h' ahd else ahd +^ bhd in
		    (match btl with
		      [] ->
		        let c' = (hd c *^ t) :: (tl c) in
		        aux c' atl btl d ttl
		    | bhd' :: btl' ->
		        let b' = (bhd' *^ t) :: btl' in
		        aux c atl b' d ttl
		    )
	        )
	    | Some vai ->
	        aux c (vai :: a) (unit :: b) d ttl
      in
      aux c a b d (tl t)

  let phase1_parallel trees =
    let num_proc = List.length trees in
    List.mapi (phase1 num_proc) trees |> Array.of_list

  let phase2 phase1_return =
    let num_proc = Array.length phase1_return
    and phase1_return = Array.to_list phase1_return in
    let depths = List.mapi (fun i r -> i, r.d) phase1_return
    and max_depth = max_int in
    let rec aux stack acc = function
        [] -> acc
      | (i, di) :: tl ->
          let rec aux2 d stack acc =
            let (ps, ds) = Stack2.peek stack in
            if di < ds then aux2 ds (Stack2.remove stack) ((ps, i, ds, d) :: acc)
            else d, stack, acc
          in
          let d, stack, acc = aux2 max_depth stack acc in
          let (ps, ds) = Stack2.peek stack in
          if di = ds then
            let stack = Stack2.remove stack |> Stack2.push (i, di)
            and acc = (num_proc, num_proc, di, ds) :: (ps, i, ds, d) :: acc in
            aux stack acc tl (* num_proc, num_proc : dummy *)
          else
            let stack = Stack2.push (i, di) stack
            and acc = (ps, i, di, d) :: acc in
            aux stack acc tl
    in
    aux (Stack2.singleton (List.hd depths)) [] (List.tl depths)
                |> List.filter (fun m -> quad1 m <> num_proc) (* remove dummy *)

  let get_triplet phase1_return (pl, pr, du, dl) =
    let num_proc = Array.length phase1_return
    and pleft, pright = phase1_return.(pl), phase1_return.(pr) in
    let rec aux d acc =
      if !debug then Printf.printf "(pl, pr, du, dl), d : (%d, %d, %d, %d), %d\n" pl pr du dl d;
      if d >= dl || d - pleft.d >= Array.length pleft.ab then acc
      else
        let triplet =
          let a, b = pleft.ab.(d - pleft.d)
          and c = pright.c.(d - pright.d + if pr = num_proc - 1 then 0 else 1)
          in
          if d - pleft.d = 0 && num_proc <> 1 && (0 < pl && pl < num_proc - 1) then
            Addition (pleft.c.(0), (a, b, c))
          else
            Normal (a, b, c)
        in
        aux (d + 1) (triplet :: acc)
    in
    aux du []
end

module Int = struct
  type t1 = Unit | Value of int
  type t2 = t1
  let h' = identity
  let ( *^ ) x y = match x, y with
      Value x, Value y -> Value (max x y)
    | Unit, Value y -> Value y
    | Value x, Unit -> Value x
    | Unit, Unit -> Unit
  let ( +^ ) x y = match x, y with
      Value x, Value y -> Value (x + y)
    | _ -> Unit
  let unit = Unit
  let unit_inv = Unit
end

open Int
module M = Make(Int)

let tree =
  let open TreeSerialize in
  Node (Value 3,
  [Leaf (Value 4);
  Node (Value (-5),
  [Node (Value 6,
  [Leaf (Value (-2)); Leaf (Value 8); Node (Value (-1), [Leaf (Value 4)])]);
  Leaf (Value 1)]);
  Leaf (Value 5); Node (Value 2, [Leaf (Value (-6))])])

let tree2 =
  let open TreeSerialize in
  Node (Value 1,
  [
    Node (Value 1, [
      Leaf (Value 1);
      Leaf (Value 1);
      Leaf (Value 1)
    ]);
    Leaf (Value 1);
    Node (Value 1, [
      Leaf (Value 1);
      Leaf (Value 1);
      Node (Value 1, [Node (Value 1, [Leaf (Value 1)])]);
      Node (Value 1, [
        Node (Value 1, [
          Node (Value 1, [
            Leaf (Value 1);
            Leaf (Value 1)
          ])
        ]);
        Leaf (Value 1)
      ])
    ]);
    Leaf (Value 1);
    Leaf (Value 1)
  ])

let tree3 =
  let open TreeSerialize in
  Node (Value 3,
  [
    Node (Value 6,
    [
      Node (Value 2, [Leaf (Value 1)]);
      Leaf (Value 4);
    ]);
    Leaf (Value 5);
    Node (Value (-1), [Leaf (Value 3)])
  ])

let tree3 =
  let open TreeSerialize in
  Node (Value 7,
  [
    Node (Value 6,
    [
      Node (Value 2, [Leaf (Value 0); Leaf (Value 1)]);
      Leaf (Value 4);
    ]);
    Leaf (Value 9);
    Node (Value 5, [Leaf (Value 3)])
  ])
