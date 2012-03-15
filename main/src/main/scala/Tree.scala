import scala.collection.mutable.ArraySeq

trait TreeElmConstructable[A] {
  def isRoot: Boolean

  def isChild(c: A): Boolean

  def isParent (p: A): Boolean

  def isLeaf: Boolean
}

class Tree[A](val value: A, val children: List[Tree[A]]) {
  def map[B] (f: A => B): Tree[B] = new Tree(f(value), children map (_.map(f)))

  def isLeaf = children.isEmpty

  def contract(compress: (A, A) => A, rake: (A, A) => A): A = {
    def aux(t: Tree[A]): A = {
      DebugPrinter.println(t)
      t.children match {
        case hd :: tl if hd.isLeaf=> { // rake
          DebugPrinter.println("rake")
          aux(new Tree(rake(t.value, hd.value), tl))
        }
        case hd :: Nil => {// compress
          DebugPrinter.println("compress")
          aux(new Tree(compress(t.value, hd.value), hd.children))
        }
        case hd :: tl => {// only nodes and no leaves
          DebugPrinter.println("go deeper")
          aux(new Tree(rake(t.value, aux(hd)), tl))
        }
        case Nil => {// compressed all nodes
          t.value
        }
      }
    }
    aux(this)
  }

  def contractSequential(rake: (A, A) => A, prep: A => A): A = {
    def aux(t: Tree[A]): A = {
      DebugPrinter.println(t)
      t.children match {
        case hd :: tl if hd.isLeaf=> {
          DebugPrinter.println("rake")
          aux(new Tree(rake(t.value, prep(hd.value)), tl))
        }
        case hd :: tl => {
          DebugPrinter.println("go deeper")
          aux(new Tree(rake(t.value, aux(hd)), tl))
        }
        case Nil => {
          prep(t.value)
        }
      }
    }
    aux(this)
  }

  override def toString = "Node(" + value.toString + ", [" + ((children map (_.toString)) mkString ", ") + "])"
}

object Tree {
  def construct[A <: TreeElmConstructable[A]](xs: List[A]): Tree[A] = {
    def aux(xs: List[A])(value: A): Tree[A] = {
      if (value.isLeaf)
        new Tree(value, Nil)
      else {
        val (children, rest) = xs partition (value.isChild)
        new Tree(value, children map (aux(rest)(_)))
      }
    }
    xs find (_.isRoot) match {
      case Some(root) => aux(xs)(root)
      case None => throw new IllegalArgumentException("root not found")
    }
  }

  def contractDirect[A <: TreeElmConstructable[A], B](rake: (B, B) => B, repr: B => B)(xs: Array[(A, B)]) = {
 //     val xs0 = xs sortWith ((c: (A, B), p: (A, B)) => (p._1 isParent c._1))

    val num = xs.length
    val constrs = xs map (_._1)
    val values = xs map (_._2)

    val rootIndex = constrs lastIndexWhere (_.isRoot)

    val initLeaves: List[Int] = List range (0, num) filter (constrs(_).isLeaf)
    val nodeToNumChildren = Array.fill(num)(0)

    def findParent(c: Int) = {
      val p = constrs indexWhere ((p: A) => constrs(c) isParent p, 0)
      if(p != -1) nodeToNumChildren(p) = nodeToNumChildren(p) + 1
      p
    }

    val nodeToParent = Array.range(0, num) map findParent

    def rakeWrap(newLeaves: List[Int], leaf: Int) = {
      val p = nodeToParent(leaf)
      values(p) = rake(values(p), values(leaf))
      val newNumChildren = nodeToNumChildren(p) - 1
      nodeToNumChildren(p) = newNumChildren
      if(newNumChildren == 0)
        {
          values(p) = repr(values(p))
          p :: newLeaves
        }
      else newLeaves
    }

    def aux(leaves: List[Int]): B = {
      leaves match {
        case List(i) if i == rootIndex => repr(values(rootIndex))
        case _ => aux(leaves.foldLeft(Nil: List[Int])(rakeWrap))
      }
    }

    initLeaves foreach ((i: Int) => values(i) = repr(values(i)))
    aux(initLeaves)
  }

}
