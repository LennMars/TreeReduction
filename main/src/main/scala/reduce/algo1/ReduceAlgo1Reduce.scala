import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Mapper._
import scala.collection.JavaConversions._
import scala.annotation.tailrec

class ReduceAlgo1Reduce extends Reducer[NullWritable, Text, NullWritable, Text] {

  override def reduce(key: NullWritable,
                      values: java.lang.Iterable[Text],
                      context: Reducer[NullWritable, Text, NullWritable, Text]#Context) {
    val conf = context.getConfiguration
    val reduceClassName = conf.get("reduceClassName")
    val vObj = Class.forName(reduceClassName).getConstructors.apply(0).newInstance().asInstanceOf[AbstractReduce]
    val hNFObj = Class.forName("AbstractReduce$HillNormalForm$").getConstructors.apply(0).newInstance(vObj)

    def tolist(xs: Iterator[Text]): List[AbstractReduce#HillNormalForm] = {
      val ofString = hNFObj.getClass.getMethod("ofString", classOf[String])
      @tailrec
      def aux(acc: List[AbstractReduce#HillNormalForm]): List[AbstractReduce#HillNormalForm] = {
        if (xs.hasNext) aux(ofString.invoke(hNFObj, xs.next.toString).asInstanceOf[AbstractReduce#HillNormalForm] :: acc)
        else acc.sortBy(_.offset)
      }
      aux(Nil)
    }

    val hNFs = tolist(values.iterator)

    DebugPrinter.println("hNF: \n" + hNFs.mkString("\n"))

    val mergeMeth = hNFObj.getClass.getMethod("merge", classOf[AbstractReduce#HillNormalForm], classOf[AbstractReduce#HillNormalForm])

    def merger(l: AbstractReduce#HillNormalForm, r: AbstractReduce#HillNormalForm) = {
      mergeMeth.invoke(hNFObj, l, r).asInstanceOf[AbstractReduce#HillNormalForm]
    }

    val phase2 = hNFs.reduceLeft(merger)
    val ans = (phase2.cs)(0).toString

    println("answer: " + (phase2.cs)(0))

    context.write(NullWritable.get, new Text(ans))
  }
}
