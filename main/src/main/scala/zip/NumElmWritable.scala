import java.io.DataOutput
import java.io.DataInput
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Mapper._

class NumElmWritable(var offset: Long, var num: Int) extends WritableComparable[NumElmWritable] {

  def this() = this(0, 0)

  def get = (offset, num)

  def set(o: Long, n: Int) = {
    offset = o
    num = n
  }

  override def write(out: DataOutput) {
    out writeLong(offset)
    out writeInt(num)
  }

  override def readFields(in: DataInput) {
    offset = in readLong()
    num = in readInt()
  }

  override def compareTo(x: NumElmWritable) = {
    val sign: Int => Int = scala.math.signum(_)
    val signl: Long => Int = scala.math.signum(_).toInt
    sign(signl(offset - x.offset) * 2 + sign(num - x.num))
  }

  override def equals(o: Any) = {
    o match {
      case obj: NumElmWritable => offset == obj.offset && num == obj.num
      case _ => false
    }
  }

  override def hashCode = {
    (offset, num).hashCode
  }

  override def toString() = offset + "\t" + num

  class Comparator extends WritableComparator(classOf[HNFShapeWritable]) {
    override def compare(b1: Array[Byte], s1: Int, l1: Int,
                         b2: Array[Byte], s2: Int, l2: Int): Int = {
      val ox = WritableComparator.readLong(b1, s1)
      val oy = WritableComparator.readLong(b2, s2)
      val read = WritableComparator.readInt(_, _)
      if (ox != oy) (ox - oy).toInt
      else read(b1, s1 + 8) - read(b2, s2 + 8)
    }
  }
}
