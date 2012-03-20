import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Mapper._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.MapFile.Reader
import org.apache.hadoop.fs.FileSystem
import scala.collection.immutable.HashSet
import scala.annotation.tailrec

class ReduceAlgo2Map2 extends Mapper[NullWritable,Text,MatingPairWritable,Text] {
  def addPairs(set: HashSet[MatingPairWritable], offset: Long, reader: MapFile.Reader): HashSet[MatingPairWritable] = {
    val index = new LongWritable
    val toRead = new MatingPairArrayWritable
    val add = (s: HashSet[MatingPairWritable], w: Writable) => {
      w match {
        case mp: MatingPairWritable => s + mp
        case _ =>s
      }
    }
    index.set(offset)
    if(reader.get(index, toRead) != null) toRead.get.foldLeft(set)(add)
    else set
  }

  override def map(key: NullWritable,
		   value: Text,
		   context: Mapper[NullWritable, Text, MatingPairWritable, Text]#Context)
  {
    val conf = context.getConfiguration
    val vObj = Class.forName(conf.get("reduceClassName")).getConstructors.apply(0).newInstance().asInstanceOf[AbstractReduce]
    val hNFObj = Class.forName("AbstractReduce$HillNormalForm$").getConstructors.apply(0).newInstance(vObj)
    val ofStringMeth = hNFObj.getClass.getMethod("ofString", classOf[String])

    val hNF = ofStringMeth.invoke(hNFObj, value.toString).asInstanceOf[AbstractReduce#HillNormalForm]
    val (myOffset, cs0, abs) = (hNF.offset, hNF.cs, hNF.abs)

    val fs = FileSystem.get(conf)

    val offsetDepthMap = new MapFile.Reader(fs, conf.get("offsetDepthMapFile"), conf)
    val offsetWritable = new LongWritable
    val depthWritable = new IntWritable

    offsetDepthMap.finalKey(offsetWritable)
    val maxOffset = offsetWritable.get

    offsetWritable.set(myOffset)
    offsetDepthMap.get(offsetWritable, depthWritable)
    val myDepth = depthWritable.get

    val isLast = myOffset == maxOffset

    val nextOffset: Option[Long] =
      if (isLast) None
      else {
        offsetDepthMap.next(offsetWritable, depthWritable) // depends on above set and get
        Some(offsetWritable.get)
      }

    DebugPrinter.println("offset(me/next/max), depth: " + myOffset + "/" + nextOffset + "/" + maxOffset + ", " + myDepth)

    val matingPairsLeft = new MapFile.Reader(fs, conf.get("matingPairsLeftFile"), conf)
    val matingPairsRight = new MapFile.Reader(fs, conf.get("matingPairsRightFile"), conf)

    val cs = if(myOffset == 0 || isLast) cs0.tail else cs0

    val setl = addPairs(HashSet.empty, myOffset, matingPairsLeft)
    val setlr = addPairs(setl, myOffset, matingPairsRight)
    val setlrn = nextOffset match {
      case Some(o) => addPairs(setlr, o, matingPairsRight)
      case None => setlr
    }

    val tpieceConstr = Class.forName("AbstractReduce$TripletPiece").getConstructors.apply(0)
    val tpieceText = new Text()

    def set(d: Int, piece: Either[(Option[AbstractReduce#t2], AbstractReduce#t1, AbstractReduce#t2), (AbstractReduce#t2, Boolean)]) {
      val s = tpieceConstr.newInstance(vObj, d.asInstanceOf[java.lang.Object], piece.asInstanceOf[java.lang.Object]).asInstanceOf[AbstractReduce#TripletPiece].toShortString
      DebugPrinter.println("Map2.set: " + s)
      tpieceText.set(s)
    }

    def getTriplet(mp: MatingPairWritable) = {
      val (pl, pr, du, dl) = (mp.offL, mp.offR, mp.du, mp.dl)

      @tailrec
      def getL(d: Int) {
	if (d >= dl || d - myDepth >= abs.length) ()
	else {
	  val (a, b) = abs(d - myDepth)
          if (d == myDepth && pl != 0 && pl != maxOffset) {
            set(d, Left(Some(cs(0)), a, b))
          } else {
            set(d, Left(None, a, b))
          }
	  context.write(mp, tpieceText)
	  getL(d + 1)
	}
      }

      @tailrec
      def getR(d: Int) {
	val i = d - myDepth + (if (pr == maxOffset) 0 else 1)
	if (d >= dl || i >= cs.length) ()
	else {
          set(d, Right((cs(i), true)))
	  context.write(mp, tpieceText)
	  getR(d + 1)
	}
      }

      def getR2() {
        if (dl == myDepth && abs.isEmpty) {
          set(myDepth - 1, Right((cs(0), false)))
          context.write(mp, tpieceText)
        }
      }

      if (myOffset == pl) getL(du)
      else if (myOffset == pr) getR(du)
      else nextOffset match {
        case Some(o) => if (o == pr - 1) getR2()
        case None => ()
      }
    }

    setlrn foreach ((mp: MatingPairWritable) => getTriplet(mp))

    offsetDepthMap.close()
    matingPairsLeft.close()
    matingPairsRight.close()
  }
}
