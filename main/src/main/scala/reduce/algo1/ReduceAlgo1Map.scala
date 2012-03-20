import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Mapper._
import scala.collection.JavaConversions._
import scala.annotation.tailrec

class ReduceAlgo1Map extends Mapper[LongWritable, Text, NullWritable, Text] {
  override def map(key: LongWritable,
                   value0: Text,
                   context: Mapper[LongWritable, Text, NullWritable, Text]#Context) {
    val s = value0.toString
    if(s != "") {
      val conf = context.getConfiguration
      val v = Class.forName(conf.get("reduceClassName")).getConstructors.apply(0).newInstance().asInstanceOf[AbstractReduce]
      val o = Class.forName("AbstractReduce$TreeSerialized$").getConstructors.apply(0).newInstance(v).asInstanceOf[java.lang.Object]
      val m = o.getClass.getMethod("ofString", classOf[String], classOf[String])
      val tree0 = m.invoke(o, s, " ")
      val tree = tree0.asInstanceOf[AbstractReduce#TreeSerialized]
      val h = Class.forName("AbstractReduce$HillNormalForm$").getConstructors.apply(0).newInstance(v)
      val hNF = tree.hillNormalForm(key.get)
      DebugPrinter.println("map1: " + hNF.toString)
      context.write(NullWritable.get, new Text(hNF.toString))
    }
  }
}

