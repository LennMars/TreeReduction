import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Mapper._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.MapFile.Reader
import org.apache.hadoop.fs.FileSystem
import scala.collection.immutable.HashSet
import scala.annotation.tailrec

class ZipMap1 extends Mapper[LongWritable,Text,NullWritable,NumElmWritable] {
  override def map(key: LongWritable,
		   value: Text,
		   context: Mapper[LongWritable, Text, NullWritable, NumElmWritable]#Context)
  {
    val conf = context.getConfiguration
    val zipObj = Class.forName(conf.get("zipClassName")).getConstructors.apply(0).newInstance().asInstanceOf[AbstractZip]
    val zipInputType = conf.getInt("zipInputType", 1)
    val extractMeth = zipObj.getClass.getMethod("extract" + zipInputType, classOf[String])
    val len = extractMeth.invoke(zipObj, value.toString).asInstanceOf[List[_]].length
    val numElm = new NumElmWritable(key.get, len)
    context.write(NullWritable.get, numElm)
  }
}
