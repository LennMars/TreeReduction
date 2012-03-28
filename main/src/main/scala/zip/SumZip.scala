import scala.annotation.tailrec

class SumZip extends AbstractZip {
  type t1 = Int
  type t2 = Int
  type t3 = Int
  def extract1(s: String) = {
    def makeT1 (x: String) = x match {
      case "/" => None
      case _ => Some(t1OfString(x))
    }
    @tailrec
    def aux (ss: Array[String], i: Int, xs: List[Option[t1]]): List[Option[t1]] = {
      if (i <= 0) xs
      else aux(ss, i - 1, makeT1(ss(i - 1)) :: xs)
    }
    if(s.length != 0) {
      val ss = s.split(" ")
      aux (ss, ss.length, Nil)
    } else
      Nil
  }
  def extract2(s: String) = extract1(s)
  val zipper = (x: Int, y: Int) => x + y
  val t1OfString = (s: String) => s.toInt
  val t2OfString = t1OfString
  val t3OfString = t1OfString
  def t1ToString(x: Int) = x.toString
  def t2ToString(x: Int) = x.toString
  def t3ToString(x: Int) = x.toString
}
