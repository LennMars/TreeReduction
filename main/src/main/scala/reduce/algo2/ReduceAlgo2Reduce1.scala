import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.MapReduceBase
import org.apache.hadoop.mapred.Reducer
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.MapFileOutputFormat
import scala.annotation.tailrec
import scala.collection.mutable.HashMap
import scala.collection.immutable.Stack
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class ReduceAlgo2Reduce1
extends MapReduceBase
with Reducer[NullWritable, HNFShapeWritable, NullWritable, NullWritable] {

  // conf files can be used only in configure method
  var writerOffsetDepth: MapFile.Writer = null
  var writerLeft: MapFile.Writer = null
  var writerRight: MapFile.Writer = null
  val max_depth = Int.MaxValue

  override def configure(conf: JobConf) {
    val outputTmpFull = conf.get("outputTmpFull")
    val pathOffsetDepth = new Path(outputTmpFull, "offsetDepthMap")
    val pathLeft = new Path(outputTmpFull, "matingPairsLeft")
    val pathRight = new Path(outputTmpFull, "matingPairsRight")
    val fs = FileSystem.get(conf)

    writerOffsetDepth = new MapFile.Writer(conf, fs, pathOffsetDepth.toString, classOf[LongWritable], classOf[IntWritable])
    writerLeft = new MapFile.Writer(conf, fs, pathLeft.toString, classOf[LongWritable], classOf[MatingPairArrayWritable])
    writerRight = new MapFile.Writer(conf, fs, pathRight.toString, classOf[LongWritable], classOf[MatingPairArrayWritable])
  }

  def tolist(xs: Iterator[HNFShapeWritable]) = {
    @tailrec
    def aux(acc: List[(Long, Int, Int)]): List[(Long, Int, Int)] = {
      if (xs.hasNext) aux(xs.next.get :: acc)
      else acc.sortBy((x: (Long, Int, Int)) => x._1) // because of this, all elements must be retrieved first
    }
    aux(Nil)
  }

  def shapesToDepths(shapes: List[(Long, Int, Int)]): List[(Long, Int)] = {
    def aux(shapes: List[(Long, Int, Int)], absoluteDepth: Int, acc: List[(Long, Int)]): List[(Long, Int)] = {
      shapes match {
        case Nil => acc.reverse
        case (offset, csLen, absLen) :: tl => {
          val csLen2 = scala.math.max(0, csLen - 1)
          aux(tl, absoluteDepth + absLen - csLen2, (offset, absoluteDepth - csLen2) :: acc)
        }
      }
    }
    aux(shapes, 0, Nil)
  }

  def getMatingPairs(depths: List[(Long, Int)]) = {
    @tailrec
    def aux(stack: Stack[(Long, Int)], acc: List[MatingPairWritable], depths: List[(Long, Int)]): List[MatingPairWritable]
    = depths match {
      case Nil => acc
      case (pi, di) :: tl => {
        def aux2 (d: Int, stack: Stack[(Long, Int)], acc: List[MatingPairWritable])
        : (Int, Stack[(Long, Int)], List[MatingPairWritable])
        = {
          val (ps, ds) = stack.top
          if (di < ds) aux2(ds, stack.pop, (new MatingPairWritable(ps, pi, ds, d) :: acc))
          else (d, stack, acc)
        }
        val (d0, stack0, acc0) = aux2(max_depth, stack, acc)
        val (ps, ds) = stack0.top
        if (di == ds) {
          val stack1 = stack0.pop.push((pi, di))
          val acc1 = new MatingPairWritable(ps, pi, ds, d0) :: acc0
          aux(stack1, acc1, tl)
        } else {
          val stack1 = stack0.push((pi, di))
          val acc1 = new MatingPairWritable(ps, pi, di, d0) :: acc0
          aux(stack1, acc1, tl)
        }
      }
    }
    aux(Stack().push(depths.head), Nil, depths.tail)
  }

  def toMapWritable(getKey: MatingPairWritable => Long)(values: List[MatingPairWritable]): HashMap[Long, Array[Writable]] = {
    val bufmap: HashMap[Long, ListBuffer[Writable]] = HashMap.empty
    values foreach {
      (v: MatingPairWritable) => {
        val key = getKey(v)
        if (bufmap contains key) {bufmap(key) += v; ()}
        else {bufmap += ((key, ListBuffer(v))); ()}
      }
    }
    bufmap map ((kv: (Long, ListBuffer[Writable])) => (kv._1, kv._2.toArray))
  }

  def writeMap(writer: MapFile.Writer, depths: List[(Long, Int)]) {
    val k = new LongWritable
    val v = new IntWritable
    depths foreach {
      (d: (Long, Int)) =>
        k.set(d._1)
        v.set(d._2)
        writerOffsetDepth.append(k, v)
    }
  }

  def writeHashMap(writer: MapFile.Writer, hashMap: HashMap[Long, Array[Writable]]) {
    val index = new LongWritable
    val toWrite = new MatingPairArrayWritable
    hashMap.toArray.sortBy(_._1) foreach {
      (z: (Long, Array[Writable])) =>
        index.set(z._1)
        toWrite.set(z._2)
        writer.append(index, toWrite)
    }
  }

  override def reduce(key: NullWritable,
	              values: java.util.Iterator[HNFShapeWritable],
	              output: OutputCollector[NullWritable, NullWritable],
                      reporter: Reporter)
  {
    val shapes = tolist(values.asScala)
    val depths = shapesToDepths(shapes)
    writeMap(writerOffsetDepth, depths)

    DebugPrinter.println("Reduce1.shapes:")
    DebugPrinter.println(shapes.mkString(", "))
    DebugPrinter.println("Reduce1.depths:")
    DebugPrinter.println(depths.mkString(", "))

    val matingPairs = getMatingPairs(depths)
    val mapLeft = toMapWritable(_.offL)(matingPairs)
    val mapRight = toMapWritable(_.offR)(matingPairs)
    writeHashMap(writerLeft, mapLeft)
    writeHashMap(writerRight, mapRight)

    DebugPrinter.println("Reduce1.matingPairs:")
    DebugPrinter.println(matingPairs.mkString("\n"))
  }

  override def close {
    writerOffsetDepth.close()
    writerLeft.close()
    writerRight.close()
  }
}
