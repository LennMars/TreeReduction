import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConverters._
import scala.annotation.tailrec

class ZipReduce3 extends Reducer[LongWritable, Text, LongWritable, Text] {

  override def reduce(key: LongWritable,
	              values: java.lang.Iterable[Text],
	              context: Reducer[LongWritable, Text, LongWritable, Text]#Context)
  {
    val conf = context.getConfiguration
  }
}

