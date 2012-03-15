import scala.collection.immutable.Stack

object Algo2Reduce1Test {
  val reduce = new Algo2Reduce1
  val depths: List[(Long, Int)] = List((0,0), (1,3), (2,1), (3,0))
  val phase2result = reduce.getMatingPairs(depths)
  println(phase2result)
}
