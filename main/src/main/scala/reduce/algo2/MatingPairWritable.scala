import java.io.DataOutput
import java.io.DataInput
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Mapper._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.io.{BufferedReader, InputStreamReader}
import org.apache.hadoop.io.compress.CompressionCodecFactory

class MatingPairWritable(var offL: Long, var offR: Long, var du: Int, var dl: Int) extends WritableComparable[MatingPairWritable] with TreeElmConstructable[MatingPairWritable]{

  def this() = {
    this(0, 0, 0, 0)
    WritableComparator.define(classOf[MatingPairWritable], new Comparator)
  }

  def get = (offL, offR, du, dl)

  def set(q: (Long, Long, Int, Int)) {
    offL = q._1
    offR = q._2
    du = q._3
    dl= q._4
  }

  def isRoot = du == 0

  def isChild(c: MatingPairWritable) = {
    val (coffL, coffR, cdu, cdl) = c.get
    (dl == cdu) && (offL <= coffL && coffL < offR) && (offL < coffR && coffR <= offR)
  }

  def isParent(p: MatingPairWritable) = p isChild this

  def isLeaf = dl == Int.MaxValue

  override def write(out: DataOutput) {
    out writeLong(offL)
    out writeLong(offR)
    out writeInt(du)
    out writeInt(dl)
  }

  override def readFields(in: DataInput) {
    offL = in readLong()
    offR = in readLong()
    du = in readInt()
    dl = in readInt()
  }

  override def compareTo(x: MatingPairWritable) = {
    val sign: Int => Int = scala.math.signum(_)
    val signl: Long => Int = scala.math.signum(_).toInt
    sign(signl(offL - x.offL) * 8 + signl(offR - x.offR) * 4 + sign(du - x.du) * 2 + sign(dl - x.dl))
  }

  override def equals(o: Any) = {
    o match {
      case obj: MatingPairWritable =>
        offL == obj.offL && offR == obj.offR && du == obj.du && dl == obj.dl
      case _ => false
    }
  }

  override def hashCode = {
    (offL, offR, du, dl).hashCode
  }

  override def toString() = offL + "\t" + offR + "\t" + du + "\t" + dl

  class Comparator extends WritableComparator(classOf[MatingPairWritable]) {
    override def compare(b1: Array[Byte], s1: Int, l1: Int,
                         b2: Array[Byte], s2: Int, l2: Int): Int = {
      val readInt = WritableComparator.readInt(_, _)
      val readLong = WritableComparator.readLong(_, _)
      var x = 0
      val compareOnce: Int => Unit = {
          case 0 => x = (readLong(b1, s1) - readLong(b2, s2)).toInt
          case 1 => x = (readLong(b1, s1 + 8) - readLong(b2, s2 + 8)).toInt
          case 2 => x = readInt(b1, s1 + 16) - readInt(b2, s2 + 16)
          case 3 => x = readInt(b1, s1 + 20) - readInt(b2, s2 + 20)
      }

      if({compareOnce(0); x != 0}) x
      else if({compareOnce(1); x != 0}) x
      else if({compareOnce(2); x != 0}) x
      else {compareOnce(3); x}
    }
  }

}

object MatingPairWritable {
  private def readLines(paths: List[Path], conf: Configuration): List[String] = {
    def auxIn(br: BufferedReader, acc: List[String]): List[String] = {
      val s = br.readLine
      if (s == null) { // end of the stream
        br.close()
        acc
      }
      else
        auxIn(br, s :: acc)
    }

    def aux(paths: List[Path], acc: List[String]): List[String] = {
      paths match {
        case path :: tl =>
          val fs = path.getFileSystem(conf)
        val factory = new CompressionCodecFactory(conf)
        val codec = factory.getCodec(path)
        val is = if(codec == null) fs.open(path)
                 else codec.createInputStream(fs.open(path))
        val isr = new InputStreamReader(is)
        aux(tl, auxIn(new BufferedReader(isr), acc))
        case Nil =>
          acc
      }
    }

    aux(paths, Nil)
  }

  private def filterSomeMap[A, B](xs: List[A], f: A => B) = {
    val g = (x: A) => try Some(f(x)) catch {case _ => None}
    val folder = (ys: List[B], y: Option[B]) => y match {
      case Some(y) => y :: ys
      case None => ys
    }
    xs.map(g).foldLeft(Nil: List[B])(folder)
  }

  private def readOne(s: String): MatingPairWritable = {
    val s0 = s.split("\t")
    val sP = s0.take(2).map(_.toLong)
    val sD = s0.drop(2).map(_.toInt)
    new MatingPairWritable(sP(0), sP(1), sD(0), sD(1))
  }

  def read(ss: List[String]): List[MatingPairWritable] = filterSomeMap(ss, readOne)

  def readFromPaths(paths: List[Path], conf: Configuration) = read(readLines(paths, conf))

  def readFromPath(path: Path, conf: Configuration) = read(readLines(List(path), conf))

  def readWithValueOne[A](ofString: String => A)(s: String): (MatingPairWritable, A) = {
    val ss = s.split("\t")
    val v = ofString(ss(4))
    (new MatingPairWritable(ss(0).toLong, ss(1).toLong, ss(2).toInt, ss(3).toInt), v)
  }

  def readWithValue[A](ss: List[String], ofString: String => A): List[(MatingPairWritable, A)] = {
    filterSomeMap(ss, readWithValueOne(ofString))
  }

  def readWithValueFromPaths[A](paths: List[Path], conf: Configuration, ofString: String => A): List[(MatingPairWritable, A)] = {
    readWithValue(readLines(paths, conf), ofString)
  }
}
