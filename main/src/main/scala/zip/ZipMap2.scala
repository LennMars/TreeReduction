import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Mapper._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.MapFile.Reader
import org.apache.hadoop.fs.FileSystem
import scala.collection.immutable.HashSet
import scala.annotation.tailrec

class ZipMap2 extends Mapper[LongWritable,Text,LongWritable,TextOptionWritable] {

  override def map(key: LongWritable,
		   value: Text,
		   context: Mapper[LongWritable, Text, LongWritable, TextOptionWritable]#Context)
  {
    val conf = context.getConfiguration

    val zipObj = Class.forName(conf.get("zipClassName")).getConstructors.apply(0).newInstance().asInstanceOf[AbstractZip]
    val zipInputType = conf.getInt("zipInputType", 1)
    val extractMeth = zipObj.getClass.getMethod("extract" + zipInputType, classOf[String])
    val xs = extractMeth.invoke(zipObj, value.toString).asInstanceOf[List[Option[java.lang.Object]]]

    val toStringMeth = zipObj.getClass.getMethods.find(_.getName == "t" + zipInputType + "ToString").get
    val numElm = new LongWritable
    val isLeft = zipInputType == 1
    val text = new TextOptionWritable

    def output(xs: List[Option[java.lang.Object]], acc: Long) {
      xs match {
        case Nil => {()}
        case l :: r =>
          l match {
            case Some(x) =>
              text.set(isLeft, true, toStringMeth.invoke(zipObj, x).asInstanceOf[String])
            case None =>
              text.set(isLeft, false, "")
          }
          numElm.set(acc)
          context.write(numElm, text)
          output(r, acc + 1)
      }
    }

    val fs = FileSystem.get(conf)
    val offsetNumElmMap = new MapFile.Reader(fs, conf.get("offsetNumElmMap"), conf)
    val numElmBase = new LongWritable
    offsetNumElmMap.get(key, numElmBase)
    output(xs, numElmBase.get)
  }
}

