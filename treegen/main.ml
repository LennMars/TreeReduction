open Util
open TreeReduction

let pron_num = ref "3"
let open_num = ref "10"
let deepness = ref 0.7
let is_long = ref false
let is_elm_int = ref true

let args =
  let open Arg in [
    "-r", Int Random.init, "random seed initialization with specified number";
    "-l", Set is_long, "long mode on";
    "-m", Clear is_elm_int, "matrix generate mode on";
    "-p", Set_string pron_num, "set #processors";
    "-n", Set_string open_num, "set #elements in tree";
    "-d", Set_float deepness, "set deepness";
  ]

let usage_msg = Printf.sprintf "Usage: %s [-rlm] [-p num_proc] [-n num_elm] [-d deepness]\n" Sys.argv.(0)

let _ =
  if Array.length (Sys.argv) = 1 then
    Arg.usage args usage_msg
  else begin
    Arg.parse args ignore usage_msg;
    let open TreeGenerate in
    let num_module =
      if !is_long then (module LongN : Num)
      else (module IntN : Num)
    and elm_module =
      if !is_elm_int then (module Int : Elm)
      else (module Mat : Elm) in
    random_sequential num_module elm_module !pron_num !open_num !deepness
  end
