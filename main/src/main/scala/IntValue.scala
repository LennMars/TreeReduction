object IntValue extends AbstractValueMonotype {
  type t1 = Int
  val plus = (_: Int) + (_: Int)
  val mult = (_: Int) * (_: Int)
  override val dist = {  // unnecessary actuary
    (au: Int, bu: Int, cu: Int) =>
    (al: Int, bl: Int, cl: Int) =>
      (au + bu * al * cu, bu * bl, cl * cu)
  }
  val t1OfString = (s: String) => s.toInt
  val t1ToString = (v: Int) => v.toString
  val t2OfString = t1OfString
  val t2ToString = t1ToString
  val unitT2 = 1
  val isUnit = (t: t2) => t == 1
}
