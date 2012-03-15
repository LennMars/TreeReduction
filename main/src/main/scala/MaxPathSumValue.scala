class MaxPathSumValue extends AbstractValue{
  type t1 = Int
  type t2 = Option[Int] // None means unit of max i.e. -infinity
  val hom = (x: t1) => Some(x)
  val plus = (x: t1, y: t2) => y match {
    case Some(y) => Some(x + y)
    case None => None
  }
  val mult = (x: t2, y: t2) => (x, y) match {
    case (Some(x), Some(y)) => Some(scala.math.max(x, y))
    case (Some(x), None) => Some(x)
    case (None, Some(y)) => Some(y)
    case (None, None) => None
  }
  val dist = (a1: t1, b1: t2, c1: t2) => (a2: t1, b2: t2, c2: t2) => {
    val plus3 = (a: t1, bu: t2, bl: t2) => mult(bl, plus(-a, bu))
    (a1 + a2, plus3(a2, b1, b2), plus3(a2, c1, c2))
  }
  val t1OfString = (s: String) => s.toInt
  val t2OfString = (s: String) =>
    if (s == "_") None
    else Some(s.toInt)
  val t1ToString = (v: t1) => v.toString
  val t2ToString = (v: t2) => v match {
    case Some(x) => x.toString
    case None => "_"
  }

  val unitT2 = None
  val isUnit = (t: t2) => t == None
}
