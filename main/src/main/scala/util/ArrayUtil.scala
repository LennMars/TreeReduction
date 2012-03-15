import scala.collection.mutable.ArraySeq

object ArrayUtil {
  def mapi[A, B](f: Int => A => B)(xs: ArraySeq[A]): ArraySeq[B] = {
    val xs0 = xs.zipWithIndex
    xs0 map ((xi: (A, Int)) => f(xi._2)(xi._1))
  }

  def foreachi[A](f: Int => A => Unit)(xs: ArraySeq[A]) = {
    mapi(f)(xs)
    ()
  }
}
