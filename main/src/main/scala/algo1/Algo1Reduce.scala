import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Mapper._
import scala.collection.JavaConversions._
import scala.annotation.tailrec

class Algo1Reduce extends Reducer[NullWritable, Text, NullWritable, Text] {

  override def reduce(key: NullWritable,
                      values: java.lang.Iterable[Text],
                      context: Reducer[NullWritable, Text, NullWritable, Text]#Context) {
    val conf = context.getConfiguration
    val valueClassName = conf.get("valueClassName")
    val vObj = Class.forName(valueClassName).getConstructors.apply(0).newInstance().asInstanceOf[AbstractValue]
    val hNFObj = Class.forName("AbstractValue$HillNormalForm$").getConstructors.apply(0).newInstance(vObj)

    def tolist(xs: Iterator[Text]): List[AbstractValue#HillNormalForm] = {
      val ofString = hNFObj.getClass.getMethod("ofString", classOf[String])
      @tailrec
      def aux(acc: List[AbstractValue#HillNormalForm]): List[AbstractValue#HillNormalForm] = {
        if (xs.hasNext) aux(ofString.invoke(hNFObj, xs.next.toString).asInstanceOf[AbstractValue#HillNormalForm] :: acc)
        else acc.sortBy(_.offset)
      }
      aux(Nil)
    }

    val hNFs = tolist(values.iterator)

    DebugPrinter.println("hNF: \n" + hNFs.mkString("\n"))

    val mergeMeth = hNFObj.getClass.getMethod("merge", classOf[AbstractValue#HillNormalForm], classOf[AbstractValue#HillNormalForm])

    def merger(l: AbstractValue#HillNormalForm, r: AbstractValue#HillNormalForm) = {
      mergeMeth.invoke(hNFObj, l, r).asInstanceOf[AbstractValue#HillNormalForm]
    }

    val phase2 = hNFs.reduceLeft(merger)
    val ans = (phase2.cs)(0).toString

    println("answer: " + (phase2.cs)(0))

    context.write(NullWritable.get, new Text(ans))
  }
}
