import scala.collection.immutable.ListMap

object TreeTest {
  val m = Int.MaxValue
  val x1 = new MatingPairWritable(0, 3, 0, 1)
  val x2 = new MatingPairWritable(2, 3, 1, m)
  val x3 = new MatingPairWritable(0, 2, 1, 3)
  val x4 = new MatingPairWritable(1, 2, 3, m)
  val x5 = new MatingPairWritable(0, 1, 3, m)
  val xs = List(x1, x2, x3, x4, x5)
  val tree = Tree.construct(xs)
  val map = ListMap((x1, 1), (x2, 2), (x3, 3), (x4, 4), (x5, 5))
  val valtree = tree.map((x: MatingPairWritable) => map.getOrElse(x, 0))
  val ans = valtree.contract(_+_, _+_)
}
