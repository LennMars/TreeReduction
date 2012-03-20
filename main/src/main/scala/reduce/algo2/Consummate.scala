import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

object Consummate {
  def exec(paths: List[Path], conf: Configuration) = {
    val reduceObj = Class.forName(conf.get("reduceClassName")).getConstructors.apply(0).newInstance().asInstanceOf[AbstractReduce]
    val tCompanionObj = Class.forName("AbstractReduce$Triplet$").getConstructors.apply(0).newInstance(reduceObj)
    val ofStringMeth = tCompanionObj.getClass.getMethod("ofString", classOf[String])
    val ofString = (s: String) => ofStringMeth.invoke(tCompanionObj, s)

    val xs = MatingPairWritable.readWithValueFromPaths(paths, conf, ofString)

    DebugPrinter.println("\nremaining triplets: ")
    xs foreach DebugPrinter.println

    val tObj = Class.forName("AbstractReduce$Triplet").getConstructors.apply(0).newInstance(reduceObj, null)
    val distWrapMeth = tObj.getClass.getMethods.find(_.getName == "distWrap").get
    val rake = (p: java.lang.Object, c: java.lang.Object) => distWrapMeth.invoke(p, c)

    val lastTriplet = Tree.contractDirect(rake, ((x: java.lang.Object) => x.asInstanceOf[AbstractReduce#Triplet].represent.asInstanceOf[java.lang.Object]))(xs toArray)
    lastTriplet.asInstanceOf[AbstractReduce#Triplet].represent
  }
}
