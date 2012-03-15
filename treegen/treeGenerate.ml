open Util
open Tree

exception End

module type Elm = sig
  type t
  val gen : unit -> t
  val to_string : t -> string
end

module Int : Elm = struct
  type t = int
  let gen () = 1
  let to_string = string_of_int
end

module Mat : Elm = struct
  type t = string
  let gen () =
    let r i = string_of_int (Random.int i) in
    r 1267 ^ r 1000000000 ^ r 1000000000 ^ r 1000000000
  let to_string = identity
end

module type Num = sig
  type t
  val zero : t
  val one : t
  val ( =: ) : t -> t -> bool
  val ( <=: ) : t -> t -> bool
  val ( <: ) : t -> t -> bool
  val ( >=: ) : t -> t -> bool
  val ( >: ) : t -> t -> bool
  val ( /: ) : t -> t -> t
  val shift_left_one : t -> t
  val succ : t -> t
  val pred : t -> t
  val to_string : t -> string
  val of_string : string -> t
end

module IntN : Num = struct
  type t = int
  let zero = 0
  let one = 1
  let ( =: ) = ( = )
  let ( <=: ) = ( <= )
  let ( <: ) = ( < )
  let ( >=: ) = ( >= )
  let ( >: ) = ( > )
  let ( /: ) = ( / )
  let shift_left_one x = x lsl 1
  let succ = ( + ) 1
  let pred = ( + ) (-1)
  let to_string = string_of_int
  let of_string = int_of_string
end

module LongN : Num = struct
  type t = int64;;
  let zero = Int64.zero
  let one = Int64.one
  let ( =: ) = ( = )
  let ( <=: ) = ( <= )
  let ( <: ) = ( < )
  let ( >=: ) = ( >= )
  let ( >: ) = ( > )
  let ( /: ) = Int64.div
  let shift_left_one x = Int64.shift_left x 1
  let succ = Int64.succ
  let pred = Int64.pred
  let to_string = Int64.to_string
  let of_string = Int64.of_string
end

let random_sequential num_module elm_module proc_num open_num deepness =
  let module N = (val num_module : Num) in
  let module E = (val elm_module : Elm) in
  let proc_num, open_num = N.of_string proc_num, N.of_string open_num in
  let open N in
  if proc_num <=: zero || open_num <=: zero || deepness < 0.0 || deepness > 1.0 then
    invalid_arg "random_sequential_long";
  let max_d = ref zero
  and ch = open_out "temp" in
  let p s = Printf.fprintf ch s in
  let rec line me_proc d opened_num =
    if me_proc >=: proc_num then
      ()
    else begin
      let rec elm d opened_num whole_num =
        if d >: !max_d then max_d := d;
        if (opened_num >=: open_num && d =: zero) (* already finished *)
          || (whole_num >: (shift_left_one open_num) /: proc_num) (* max elm per line *)
        then begin
          p "\n";
          d, opened_num end
        else if (
          opened_num <: open_num
          && (d =: one (* no closure without that for root *)
              || opened_num =: zero (* root *)
              || Random.float 1. < deepness (* deepness means tendency to going deeper *)
          ))
        then begin (* open node *)
          if not (whole_num =: zero) then p " ";
          p "%s" (E.gen () |> E.to_string);
          elm (succ d) (succ opened_num) (succ whole_num) end
        else begin (* close node *)
          if not (whole_num =: zero) then p " ";
          p "/";
          elm (pred d) opened_num (succ whole_num) end
      in
      let d, opened_num = elm d opened_num zero in
      line (succ me_proc) d opened_num (* next line *)
    end
  in
  flush stderr;
  line zero zero zero;
  Printf.eprintf "max_depth = %s\n" (N.to_string !max_d);
  close_out ch;
  Unix.rename "temp" (Printf.sprintf "p%sn%sd%.3fm%s" (N.to_string proc_num) (N.to_string open_num) deepness (N.to_string !max_d))

let random elm_gen num_leaves num_branches frac_up max_depth =
  let leaves = List.init (fun _ -> Leaf(elm_gen ())) num_leaves
  and finish acc = Node (elm_gen (), acc) in
  let rec aux d acc =
    if d <= 2 then finish acc else
      match acc with
        [] -> Leaf (elm_gen ())
      | acc when List.length acc < num_branches -> finish acc
      | acc ->
          let num_up = (List.length acc |> float) *. frac_up |> int_of_float in
          let up, rest = List.split_at num_up acc in
          let num_in_branch = max (List.length rest / num_branches) 1 in
          let rec aux2 acc = function
              [] -> acc
            | rest ->
                let branch, rest = List.split_at num_in_branch rest in
                let branch = finish branch in
                aux2 (branch :: acc) rest
        in
          aux (d - 1) (up @ aux2 [] rest)
  in
  aux max_depth leaves


