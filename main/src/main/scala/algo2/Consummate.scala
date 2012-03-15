import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

object Consummate {
  def exec(paths: List[Path], conf: Configuration) = {
    val valueObj = Class.forName(conf.get("valueClassName")).getConstructors.apply(0).newInstance().asInstanceOf[AbstractValue]
    val tCompanionObj = Class.forName("AbstractValue$Triplet$").getConstructors.apply(0).newInstance(valueObj)
    val ofStringMeth = tCompanionObj.getClass.getMethod("ofString", classOf[String])
    val ofString = (s: String) => ofStringMeth.invoke(tCompanionObj, s)

    val xs = MatingPairWritable.readWithValueFromPaths(paths, conf, ofString)

    DebugPrinter.println("\nremaining triplets: ")
    xs foreach DebugPrinter.println

    val tObj = Class.forName("AbstractValue$Triplet").getConstructors.apply(0).newInstance(valueObj, null)
    val distWrapMeth = tObj.getClass.getMethods.find(_.getName == "distWrap").get
    val rake = (p: java.lang.Object, c: java.lang.Object) => distWrapMeth.invoke(p, c)

    val lastTriplet = Tree.contractDirect(rake, ((x: java.lang.Object) => x.asInstanceOf[AbstractValue#Triplet].represent.asInstanceOf[java.lang.Object]))(xs toArray)
    lastTriplet.asInstanceOf[AbstractValue#Triplet].represent
  }
}
