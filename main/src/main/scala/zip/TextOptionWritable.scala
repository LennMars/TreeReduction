import java.io.{DataInput, DataOutput}
import org.apache.hadoop.io.{WritableComparable, WritableComparator}

class TextOptionWritable(var isLeft: Boolean, var isSome: Boolean, var text: String) extends WritableComparable[TextOptionWritable] {

  def this() = this(0, true, true)

  def get = (num, isLeft, isSome)

  def set(n: Long, il: Boolean, is: Boolean) = {
    num = n
    isLeft = il
    isSome = is
  }

  override def write(out: DataOutput) {
    out writeLong(num)
    out writeBoolean(isLeft)
    out writeBoolean(isSome)
  }

  override def readFields(in: DataInput) {
    num = in readLong()
    isLeft = in readBoolean()
    isSome = in readBoolean()
  }

  override def compareTo(x: TextOptionWritable) = scala.math.signum(num - x.num).toInt

  override def equals(o: Any) = {
    o match {
      case obj: TextOptionWritable => num == obj.num
      case _ => false
    }
  }

  override def toString = num + "\t" + isLeft + "\t" + isSome

  class Comparator extends WritableComparator(classOf[TextOptionWritable]) {
    override def compare(b1: Array[Byte], s1: Int, l1: Int,
                         b2: Array[Byte], s2: Int, l2: Int): Int = {
      val x = WritableComparator.readLong(b1, s1)
      val y = WritableComparator.readLong(b2, s2)
      (x - y).toInt
    }
  }

}
