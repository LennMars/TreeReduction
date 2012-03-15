trait AbstractValueMonotype extends AbstractValue {
  type t2 = t1
  val hom = (a: t1) => a
  val dist = {
    (au: t1, bu: t2, cu: t2) =>
    (al: t1, bl: t2, cl: t2) =>
      (plus(au, mult(bu, mult(hom(al), cu))),
       mult(bu, bl),
       mult(cl, cu))
  }
}
