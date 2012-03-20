import scala.math.BigInt

object XPathLikeReduce extends AbstractReduceMonotype {
  type t1 = BigInt
  val size = 10
  val zero = BigInt(0)
  def get(x: BigInt)(i: Int, j: Int) = x.testBit(i * size + j)
  def set(x: BigInt)(i: Int, j: Int) = x.setBit(i * size + j)

  val plus = (x: BigInt, y: BigInt) => { // matrix mult
    if(y != zero){
      def multOne(i: Int, j: Int) = {
        var ans = false
        for(k <- 0 until size){
          ans = ans || (get(x)(i, k) && get(y)(k, j))
        }
        ans
      }
      var ans = zero
      for(i <- 0 until size){
        for(j <- 0 until size) {
          ans = if(multOne(i, j)) set(ans)(i, j) else ans
        }
      }
      ans
    } else {
      x
    }
  }

  val mult = (x: BigInt, y: BigInt) => x | y

  val t1OfString = (s: String) => BigInt(s)
  val t1ToString = (x: BigInt) => x.toString
  val t2OfString = t1OfString
  val t2ToString = t1ToString

  def makeUnit(x: BigInt, n: Int): BigInt = {
    if(n == size) x
    else makeUnit(x.setBit(n * (size + 1)), n + 1)
  }
  val unitT2 = zero // makeUnit(zero, 0)
  val isUnit = (x: t2) => x == unitT2
}
