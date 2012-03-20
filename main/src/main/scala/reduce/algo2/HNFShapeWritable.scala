import java.io.DataOutput
import java.io.DataInput
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Mapper._

class HNFShapeWritable(var offset: Long, var csLen: Int, var absLen: Int) extends WritableComparable[HNFShapeWritable] {

  def this() = this(0, 0, 0)

  def get = (offset, csLen, absLen)

  def set(o: Long, c: Int, ab: Int) = {
    offset = o
    csLen = c
    absLen = ab
  }

  override def write(out: DataOutput) {
    out writeLong(offset)
    out writeInt(csLen)
    out writeInt(absLen)
  }

  override def readFields(in: DataInput) {
    offset = in readLong()
    csLen = in readInt()
    absLen = in readInt()
  }

  override def compareTo(x: HNFShapeWritable) = {
    val sign: Int => Int = scala.math.signum(_)
    val signl: Long => Int = scala.math.signum(_).toInt
    sign(signl(offset - x.offset) * 4 + sign(csLen - x.csLen) * 2 + sign(absLen - x.absLen))
  }

  override def equals(o: Any) = {
    o match {
      case obj: HNFShapeWritable => offset == obj.offset && csLen == obj.csLen && absLen == obj.absLen
      case _ => false
    }
  }

  override def hashCode = {
    (offset, csLen, absLen).hashCode
  }

  override def toString() = offset + "\t" + csLen + "\t" + absLen

  class Comparator extends WritableComparator(classOf[HNFShapeWritable]) {
    override def compare(b1: Array[Byte], s1: Int, l1: Int,
                         b2: Array[Byte], s2: Int, l2: Int): Int = {
      val ox = WritableComparator.readLong(b1, s1)
      val oy = WritableComparator.readLong(b2, s2)
      val read = WritableComparator.readInt(_, _)
      if (ox != oy) (ox - oy).toInt
      else {
        val cx = read(b1, s1 + 8)
        val cy = read(b2, s2 + 8)
        if (cx != cy) cx - cy
        else read(b1, s1 + 12) - read(b2, s2 + 12)
      }
    }
  }
}
