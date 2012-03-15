import scala.collection.mutable.ArrayBuilder
import scala.annotation.tailrec
import scala.collection.mutable.ArraySeq

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Mapper._
import scala.collection.JavaConversions._

 // TripletWritable
import java.io.DataOutput
import java.io.DataInput

trait AbstractValue {
  type t1
  type t2
  val hom: t1 => t2
  val plus: (t1, t2) => t2
  val mult: (t2, t2) => t2
  val dist: (t1, t2, t2) => (t1, t2, t2) => (t1, t2, t2)
  val t1OfString: String => t1
  val t2OfString: String => t2
  val t1ToString: t1 => String
  val t2ToString: t2 => String
  val unitT2: t2
  val isUnit: t2 => Boolean

  class TreeSerialized(val tree: List[Option[t1]]){
    private def listToArraySeq[A](xs: List[A]) = {
      (ArraySeq()) ++ xs
    }

    def hillNormalForm (offset: Long): HillNormalForm = {
      @tailrec
      def aux (c: List[t2], a: List[t1], b: List[t2], tree: List[Option[t1]]): HillNormalForm = tree match {
        case Nil => {
          val ab = a.zip(b).reverse
          new HillNormalForm(offset, listToArraySeq(c), listToArraySeq(ab))
        } case ai :: tl => {
          ai match {
            case Some(vai) => aux (c, (vai :: a), (unitT2 :: b), tl)
            case None => a match {
              case Nil => aux ((unitT2 :: c), a, b, tl)
              case ahd :: atl => {
                val (bhd, btl) = (b.head, b.tail)
                  val t = if (bhd == unitT2) hom(ahd) else plus(ahd, bhd)
                btl match {
                  case Nil => aux((mult(c.head, t) :: c.tail), atl, btl, tl)
                  case bhd_ :: btl_ => aux(c, atl, (mult(bhd_, t) :: btl_), tl)
                }
              }
            }
          }
        }
      }

      tree match {
        case Nil => new HillNormalForm(offset, ArraySeq.empty, ArraySeq.empty)
        case value :: tl => value match {
          case Some(a0) => aux(List(unitT2), List(a0), List(unitT2), tl)
          case None => aux(List(unitT2, unitT2), Nil, Nil, tl)
        }
      }
    }
  }

  object TreeSerialized{
    def ofString (s: String, sep: String): TreeSerialized = {
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
        val ss = s.split(sep)
        new TreeSerialized(aux (ss, ss.length, Nil))
      } else
        new TreeSerialized(Nil)
    }
  }

  class HillNormalForm(val offset: Long, val cs: ArraySeq[t2], val abs: ArraySeq[(t1, t2)]){
    override def toString: String = {
      val out_c = cs.map(t2ToString(_))
      val a = abs.map(_._1)
      val out_a = abs.map(_._1).map(t1ToString(_))
      val out_b = abs.map(_._2).map(t2ToString(_))
      def concat(s: StringBuilder, ss: ArraySeq[String], sep_inner: String, sep_outer: String) = {
        for (i <- 0 to ss.length - 2) {
          s.append(ss(i))
          s.append(sep_inner)
        }
        if (!ss.isEmpty) s.append(ss.last)
        s.append(sep_outer)
        s
      }
      val s = new StringBuilder()
      s.append(offset + "\t")
      concat(s, out_c, " ", "\t")
      concat(s, out_a, " ", "\t")
      concat(s, out_b, " ", "")
      s.toString
    }

    def merge(right: HillNormalForm): HillNormalForm = {
      // DebugPrinter.print("merge: " + this + ", " + right + " =>\n")
      assert(offset < right.offset)

      def geta(hNF: HillNormalForm)(i: Int) = (hNF.abs)(i)._1
      def getb(hNF: HillNormalForm)(i: Int) = (hNF.abs)(i)._2
      def getc(hNF: HillNormalForm)(i: Int) = (hNF.cs)(i)

      def fill_valley(n: Int): t2 = { // merge occurs n - 1 times
        @tailrec
        def aux(i: Int, v: t2): t2 = {
          if(i >= n - 1) {
            v
          } else {
            DebugPrinter.print("merging: ")
            val a = geta(this)(abs.length - i - 1)
            val b = getb(this)(abs.length - i - 1)
            val c = getc(right)(right.cs.length - i - 1)
            val o = plus(a, mult(b, mult(v, c)))
            DebugPrinter.println("(" + a + ", " + b + ", " + c + "), " + v + " -> " + o)
            aux(i + 1, o)
          }
        }
        aux(0, unitT2)
      }
      val ans = if(abs.length > scala.math.max(0, right.cs.length - 1)) {
        val n = right.cs.length
        val abmid: ArraySeq[(t1, t2)] = if(n != 0) {
          val amid = (this.abs)(this.abs.length - n)._1
          val bmid = {
            val filled_valley = fill_valley(n)
            val leftVal = getb(this)(this.abs.length - n)
            val rightVal = getc(right)(0)
            mult(leftVal, mult(filled_valley, rightVal))
          }
          ArraySeq((amid, bmid))
        } else {
          ArraySeq()
        }
        val absNew = ArraySeq.concat((abs) dropRight n, abmid, right.abs)
        new HillNormalForm(right.offset, cs, absNew)
      } else {
        val n = abs.length + 1
        val cmid: ArraySeq[t2] = if(n != 0) {
          val filled_valley = fill_valley(n)
          val leftVal = if(cs.length > 0) getc(this)(0) else unitT2
          val rightVal = if(right.cs.length >= n) getc(right)(right.cs.length - n) else unitT2
          ArraySeq(mult(leftVal, mult(filled_valley, rightVal)))
        } else {
          ArraySeq()
        }
        val absNew = right.abs
        val csNew = ArraySeq.concat((right.cs) dropRight n, cmid, cs drop 1)
        new HillNormalForm(right.offset, csNew, absNew)
      }

      DebugPrinter.println(ans)
      ans
    }

    def shape = (cs.length, abs.length)
  }

  object HillNormalForm{
    def ofString(s: String): HillNormalForm = {
      val empty: ArraySeq[String] = ArraySeq.empty
      val ss = empty ++ s.split("\t", 4)
      val sss = ss.map((s: String) => if (s != "") empty ++ s.split(" ") else empty)
      assert(sss(2).length == sss(3).length) // a and b have 1on1 relation
      val makeT1 = (s: String) => t1OfString(s)
      val makeT2 = (s: String) => t2OfString(s)
      val (offset, c, a, b) = (ss(0).toLong, sss(1).map(makeT2), sss(2).map(makeT1), sss(3).map(makeT2))
        new HillNormalForm(offset, c, a.zip(b))
    }

    def merge(l: HillNormalForm, r: HillNormalForm) = l.merge(r)
  }

  class Triplet(val value: Either[(Option[t2], t1, t2, t2), t2]){
    def distWrap(tl: Triplet): Triplet = {
      DebugPrinter.print("dist: " + this + ", " + tl + " => ")
      val ans = (value, tl.value) match {
        case (Left((eu, au, bu, cu)), Left((Some(el), al, bl, cl))) => {
          val (a, b, c) = dist(au, mult(bu, el), cu)(al, bl, cl)
          new Triplet(Left((eu, a, b, c)))
        }
        case (Left((eu, au, bu, cu)), Left((None, al, bl, cl))) => {
          val (a, b, c) = dist(au, bu, cu)(al, bl, cl)
          new Triplet(Left((eu, a, b, c)))
        }
        case (Left((Some(eu), au, bu, cu)), Right(el)) =>
          new Triplet(Left((Some(eu), au, mult(bu, el), cu)))
        case (Left((None, au, bu, cu)), Right(el)) =>
          new Triplet(Left((None, au, mult(bu, el), cu)))
        case _ =>
          throw new IllegalArgumentException("dist")
      }
      DebugPrinter.println(ans)
      ans
    }

    def represent: Triplet = {
      val ans = value match {
        case Left((e, a, b, c)) => {
          DebugPrinter.print("repr Node " + value + " => ")
          val x = if (isUnit(b) && isUnit(c)) hom(a) else plus(a, mult(b, c))
          e match {
            case Some(e) => mult(e, x)
            case None => x
          }
        }
        case Right(e) => {
          DebugPrinter.print("repr Leaf " + value + " => ")
          e
        }
      }
      DebugPrinter.println(ans)
      new Triplet(Right(ans))
    }

    override def toString = value match {
      case Left((Some(e), a, b, c)) => "aNode(" + e + ", " + a + ", " + b + ", " + c + ")"
      case Left((None, a, b, c)) => "Node(" + a + ", " + b + ", " + c + ")"
      case Right(e) => "Leaf(" + e.toString + ")"
    }
  }

  object Triplet{
    def ofString(s: String): Triplet = {
      DebugPrinter.println("received: " + s)
      if ((s take 5) == "aNode") {
          val xs = s drop 6 dropRight 1 split ", "
          val e = t2OfString(xs(0))
          val a = t1OfString(xs(1))
          val b = t2OfString(xs(2))
          val c = t2OfString(xs(3))
          new Triplet((Left(Some(e), a, b, c)))
      } else if ((s take 4) == "Node") {
          val xs = s drop 5 dropRight 1 split ", "
          val a = t1OfString(xs(0))
          val b = t2OfString(xs(1))
          val c = t2OfString(xs(2))
          new Triplet(Left((None, a, b, c)))
      } else if ((s take 4) == "Leaf") {
          val x = s drop 5 dropRight 1
          new Triplet(Right(t2OfString(x)))
      } else {
          throw new IllegalArgumentException("Triplet.ofString")
      }
    }

    def ofTripletPiece(left: TripletPiece, right: TripletPiece, aright: Option[TripletPiece]): Triplet = {
      if (left.depth != right.depth) throw new IllegalArgumentException
      (left.piece, right.piece, aright) match {
        case (Left((e, a, b)), Right((c, true)), Some(ca)) => {
          if (right.depth != ca.depth) throw new IllegalArgumentException
          ca.piece match {
            case (Right((cr, false))) => new Triplet(Left((e, a, b, mult(c, cr))))
            case _ => throw new IllegalArgumentException
          }
        }
        case (Left((e, a, b)), Right((c, true)), None) =>
          new Triplet(Left((e, a, b, c)))
        case _ =>
          throw new IllegalArgumentException
      }
    }

    def ofTripletPieces(ps: Array[TripletPiece]): List[Triplet] = {
      var left: TripletPiece = null
      var right: TripletPiece = null
      var aright: Option[TripletPiece] = None
      def updatePieces(p: TripletPiece) {
        p.piece match {
          case Left(_) => left = p
          case Right((c, true)) => right = p
          case Right((c, false)) => aright = Some(p)
        }
      }

      @tailrec
      def aux(i: Int, lastDepth: Int, acc: List[Triplet]): List[Triplet] = {
        if (i >= ps.length) { // terminate
          val t = Triplet.ofTripletPiece(left, right, aright)
          t :: acc
        }
        else if (ps(i).depth != lastDepth) {
          val t = Triplet.ofTripletPiece(left, right, aright)
          aright = None // reset pieces (left and right will be overwrote anyway)
          updatePieces(ps(i))
          aux(i + 1, ps(i).depth, t :: acc)
        } else {
          updatePieces(ps(i))
          aux(i + 1, lastDepth, acc)
        }
      }
      if (!ps.isEmpty)
        aux(0, ps(0).depth, Nil)
      else
        Nil
    }

    def reduceTriplets(ts: List[Triplet]) = {
      ts reduceOption ((tl: Triplet, tu: Triplet) => tu.distWrap(tl)) match {
        case Some(t) => Some(t.value)  // t.value
        case None => None
      }
    }
  }

  class TripletPiece(val depth: Int, val piece: Either[(Option[t2], t1, t2), (t2, Boolean)]){
    private val intToChars = (n: Int) => {
      val bs: Array[Char] = Array(0, 0)
      bs(0) = (n % 65536).toChar
      bs(1) = ((n / 65536) % 65536).toChar
      bs
    }

    def toShortString() = {
      val builder = new StringBuilder
      piece match {
        case (Left((Some(addition), a, b))) =>
          val adds = t2ToString(addition)
          val as = t1ToString(a)
          val bs = t2ToString(b)
          builder += 0.toChar // mode
          builder ++= intToChars(depth) ++= intToChars(adds.length)
          builder ++= intToChars(as.length) ++= intToChars(bs.length)
          builder ++= adds ++= as ++= bs
        case (Left((None, a, b))) =>
          val as = t1ToString(a)
          val bs = t2ToString(b)
          builder += 1.toChar // mode
          builder ++= intToChars(depth) ++= intToChars(as.length) ++= intToChars(bs.length)
          builder ++= as ++= bs
        case (Right((c, isNormal))) =>
          val cs = t2ToString(c)
          builder += (if(isNormal) 2 else 3).toChar // mode
          builder ++= intToChars(depth) ++= intToChars(cs.length)
          builder ++= cs
      }
      builder.toString
    }

    override def toString() =
      piece match {
        case (Left((Some(addition), a, b))) =>
          depth + ", Left(" + addition.toString + ", " + a.toString + ", " + b.toString + ")"
        case (Left((None, a, b))) =>
          depth + ", Left(" + a.toString + ", " + b.toString + ")"
        case (Right((c, isNormal))) =>
          if (isNormal)
            depth + ", Right(" + c.toString + ")"
          else
            depth + ", aRight(" + c.toString + ")"
      }

    def makeTriplet(right: TripletPiece): Triplet = {
      assert(depth == right.depth)
      (piece, right.piece) match {
        case (Left((e, a, b)), Right((c, isNormal))) =>
          new Triplet(Left((e, a, b, c)))
        case (Right(_), Left(_)) => // swap and evaluate again in correct order.
          DebugPrinter.print("makeTriplet: swap occured")
          right.makeTriplet(this)
        case _ =>
          throw new Exception
      }
    }
  }

  object TripletPiece {
    def ofShortString(s: String): TripletPiece = {
      val cs = s.toArray
      val charsToInt = (start: Int) => cs(start) + cs(start + 1) * 65536
      cs(0) match {
        case 0 =>
          val d = charsToInt(1)
          val addUntil = 9 + charsToInt(3)
          val aUntil = addUntil + charsToInt(5)
          val bUntil = aUntil + charsToInt(7)
          val adds = new String(cs.slice(9, addUntil))
          val as = new String(cs.slice(addUntil, aUntil))
          val bs = new String(cs.slice(aUntil, bUntil))
          DebugPrinter.println("ofShortString.case 0: " + adds + ", " + as + ", " + bs)
          val add = t2OfString(adds)
          val a = t1OfString(as)
          val b = t2OfString(bs)
          new TripletPiece(d, Left((Some(add), a, b)))
        case 1 =>
          val d = charsToInt(1)
          val aUntil = 7 + charsToInt(3)
          val bUntil = aUntil + charsToInt(5)
          val as = new String(cs.slice(7, aUntil))
          val bs = new String(cs.slice(aUntil, bUntil))
          DebugPrinter.println("ofShortString.case 1: " + as + ", " + bs)
          val a = t1OfString(as)
          val b = t2OfString(bs)
          new TripletPiece(d, Left((None, a, b)))
        case 2 | 3 =>
          val d = charsToInt(1)
          val cUntil = 5 + charsToInt(3)
          val cstr = new String(cs.slice(5, cUntil))
          DebugPrinter.println("ofShortString.case " + cs(0) + ": " + cstr)
          val c = t2OfString(cstr)
          new TripletPiece(d, Right((c, cs(0) == 2)))
      }
    }
  }

  class TripletWritable (var t: Option[Either[(Option[t2], t1, t2, t2), t2]]) extends Writable {
    def this() = {
      this(None)
    }

    def get = t

    override def toString() =
      t match {
        case Some(Left((Some(e), a, b, c))) =>
          "aNode(" + t2ToString(e) + ", " + a + ", " + t2ToString(b) + ", " + t2ToString(c) + ")"
        case Some(Left((None, a, b, c))) =>
          "Node(" + a + ", " + t2ToString(b) + ", " + t2ToString(c) + ")"
        case Some(Right(e)) =>
          "Leaf(" + t2ToString(e) + ")"
        case None =>
          "None"
      }

    override def write(out: DataOutput) {
      //     DebugPrinter.println("writing: " + this.toString)
      t match {
        case Some(Left((Some(e), a, b, c))) => {
          val eOut = t2ToString(e).getBytes
          val aOut = t1ToString(a).getBytes
          val bOut = t2ToString(b).getBytes
          val cOut = t2ToString(c).getBytes
          val (eLen, aLen, bLen, cLen) = (eOut.length, aOut.length, bOut.length, cOut.length)
          out writeByte(0) // mode
          out writeInt(eLen)
          out write(eOut)
          out writeInt(aLen)
          out write(aOut)
          out writeInt(bLen)
          out write(bOut)
          out writeInt(cLen)
          out write(cOut)}
        case Some(Left((None, a, b, c))) => {
	  val aOut = t1ToString(a).getBytes
	  val bOut = t2ToString(b).getBytes
	  val cOut = t2ToString(c).getBytes
	  val (aLen, bLen, cLen) = (aOut.length, bOut.length, cOut.length)
          out writeByte(1) // mode
	  out writeInt(aLen)
	  out write(aOut)
	  out writeInt(bLen)
	  out write(bOut)
	  out writeInt(cLen)
	  out write(cOut)}
        case Some(Right(c)) => {
	  val cOut = t2ToString(c).getBytes
	  val cLen = cOut.length
	  out writeByte(2) // mode
	  out writeInt(cLen)
	  out write(cOut)}
        case None =>
	  out writeByte(3) // mode
      }
    }

    override def readFields(in: DataInput) {
      val b = in readByte() // mode
      b match {
        case 0 => {
          val eLen = in readInt()
	  val eIn: Array[Byte] = new Array(eLen)
	  in readFully(eIn)
          val e = t2OfString(new String(eIn))
          val aLen = in readInt()
	  val aIn: Array[Byte] = new Array(aLen)
	  in readFully(aIn)
	  val a = t1OfString(new String(aIn))
	  val bLen = in readInt()
	  val bIn: Array[Byte] = new Array(bLen)
	  in readFully(bIn)
	  val b = t2OfString(new String(bIn))
	  val cLen = in readInt()
	  val cIn: Array[Byte] = new Array(cLen)
	  in readFully(cIn)
	  val c = t2OfString(new String(cIn))
	  t = Some(Left(Some(e), a, b, c))}
        case 1 => {
	  val aLen = in readInt()
	  val aIn: Array[Byte] = new Array(aLen)
	  in readFully(aIn)
	  val a = t1OfString(new String(aIn))
	  val bLen = in readInt()
	  val bIn: Array[Byte] = new Array(bLen)
	  in readFully(bIn)
	  val b = t2OfString(new String(bIn))
	  val cLen = in readInt()
	  val cIn: Array[Byte] = new Array(cLen)
	  in readFully(cIn)
	  val c = t2OfString(new String(cIn))
	  t = Some(Left(None, a, b, c))}
        case 2 => {
	  val cLen = in readInt()
	  val cIn: Array[Byte] = new Array(cLen)
	  in readFully(cIn)
	  val c = t2OfString(new String(cIn))
	  t = Some(Right(c))}
        case 3 =>
	  t = None
        case _ =>
          new IllegalArgumentException("TripletWritable.readFields")
      }
    }
  }

}
