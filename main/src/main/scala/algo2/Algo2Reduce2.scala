import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.Mapper._
import scala.collection.immutable.Stack
import scala.collection.JavaConverters._
import scala.annotation.tailrec

class Algo2Reduce2 extends Reducer[MatingPairWritable, Text, MatingPairWritable, AbstractValue#TripletWritable] {

  override def reduce(key: MatingPairWritable,
	     values: java.lang.Iterable[Text],
	     context: Reducer[MatingPairWritable, Text, MatingPairWritable, AbstractValue#TripletWritable]#Context)
  {
    val conf = context.getConfiguration
    val valueObj = Class.forName(conf.get("valueClassName")).getConstructors.apply(0).newInstance().asInstanceOf[AbstractValue]
    val tpieceObj = Class.forName("AbstractValue$TripletPiece$").getConstructors.apply(0).newInstance(valueObj)
    val ofShortStringMeth = tpieceObj.getClass.getMethod("ofShortString", classOf[String])

    val xs = values.asScala.iterator.map((t: Text) => ofShortStringMeth.invoke(tpieceObj, t.toString).asInstanceOf[AbstractValue#TripletPiece]).toArray.sortBy(_.depth) // shallow to depth order. they will be returned as ts reversed i.e. correct order.

    DebugPrinter.println("Reduce2.matingpair: " + key)
    DebugPrinter.println("Reduce2.pieces: " + xs.mkString(", "))

 //     var ts = makeTriplets(xs)
    val tObj = Class.forName("AbstractValue$Triplet$").getConstructors.apply(0).newInstance(valueObj)
    val ofTripletPiecesMeth = tObj.getClass.getMethod("ofTripletPieces", classOf[Array[AbstractValue#TripletPiece]])
    val ts0 = ofTripletPiecesMeth.invoke(tObj, xs).asInstanceOf[List[AbstractValue#Triplet]]

    val ts = if (!ts0.isEmpty && key.isLeaf) ts0.head.represent :: ts0.tail else ts0
    val reduceMeth = tObj.getClass.getMethods.find(_.getName == "reduceTriplets").get
    val t = reduceMeth.invoke(tObj, ts)

    val tWritable = Class.forName("AbstractValue$TripletWritable").getConstructors.apply(0).newInstance(valueObj, t).asInstanceOf[AbstractValue#TripletWritable]
    context.write(key, tWritable)
  }
}
