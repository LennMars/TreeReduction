import org.apache.hadoop.io._
import org.apache.hadoop.mapred.MapReduceBase
import org.apache.hadoop.mapred.Mapper
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.lib.MultipleOutputs
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.SequenceFileOutputFormat

class ReduceAlgo2Map1
extends MapReduceBase
with Mapper[LongWritable,Text,NullWritable,HNFShapeWritable] {
  var multipleOutputs: MultipleOutputs = null
  var reduceClassName = ""

  override def configure(conf: JobConf) {
    multipleOutputs = new MultipleOutputs(conf)
    reduceClassName = conf.get("reduceClassName")
  }

  override def map(key: LongWritable,
		   value0: Text,
                   output: OutputCollector[NullWritable, HNFShapeWritable],
		   reporter: Reporter)
  {
    val value = value0.toString

    if(value != "") {
      val v = Class.forName(reduceClassName).getConstructors.apply(0).newInstance().asInstanceOf[AbstractReduce]
      val o = Class.forName("AbstractReduce$TreeSerialized$").getConstructors.apply(0).newInstance(v).asInstanceOf[java.lang.Object]
      val m = o.getClass.getMethod("ofString", classOf[String], classOf[String])
      val tree = m.invoke(o, value, " ").asInstanceOf[AbstractReduce#TreeSerialized]

      val hNF = tree.hillNormalForm(key.get)

      DebugPrinter.println("map1: " + hNF.toString)

      val hNFshape = hNF.shape
      val out = new HNFShapeWritable(key.get, hNFshape._1, hNFshape._2)

      val hNFCollector = multipleOutputs.getCollector("hNF", reporter).asInstanceOf[OutputCollector[NullWritable, Text]]
      hNFCollector.collect(NullWritable.get, new Text(hNF.toString))

      output.collect(NullWritable.get, out)
    }
  }

  override def close {
    multipleOutputs.close()
  }
}
