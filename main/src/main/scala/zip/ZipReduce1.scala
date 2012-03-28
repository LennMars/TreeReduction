import org.apache.hadoop.io._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Mapper._
import scala.collection.immutable.Stack
import scala.collection.JavaConverters._
import scala.annotation.tailrec

class ZipReduce1 extends Reducer[NullWritable, NumElmWritable, NullWritable, NullWritable] {

  def write(writer: MapFile.Writer, numElms: List[(Long, Int)]) {
    val offsetWritable = new LongWritable
    val accNumElmWritable = new LongWritable
    @tailrec
    def aux(numElms: List[(Long, Int)], acc: Long) {
      numElms match {
        case Nil =>
          ()
        case (offset, num) :: tl =>
          offsetWritable.set(offset)
          accNumElmWritable.set(acc)
          writer.append(offsetWritable, accNumElmWritable)
          aux(tl, acc + num.toLong)
      }
    }
    aux(numElms, 0)
  }

  override def reduce(key: NullWritable,
	              values: java.lang.Iterable[NumElmWritable],
	              context: Reducer[NullWritable, NumElmWritable, NullWritable, NullWritable]#Context)
  {
    val conf = context.getConfiguration
    val numElms = values.asScala.iterator.map(_.get).toList.sortBy(_._1)
    val path = new Path(conf.get("offsetNumElmMap"))
    val fs = FileSystem.get(conf)
    val writer = new MapFile.Writer(conf, fs, path.toString, classOf[LongWritable], classOf[LongWritable])
    write(writer, numElms)
    writer.close()
  }
}

