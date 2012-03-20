 // common
import org.apache.hadoop.io._
import org.apache.hadoop.fs.Path
 // Job1
import org.apache.hadoop.mapred.lib.MultipleOutputs
 // Job2
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

object ReduceAlgo2Job {
  def run(input: Path, outputTmp: Path, output: Path, reduceClassName: String, deflates: Boolean) {
    DebugPrinter.println("**** mode 2 ****")
    DebugPrinter.println("*** first path running ***")
    val startJob1 = System.currentTimeMillis()
    ReduceAlgo2Job1.run(input, outputTmp, reduceClassName, deflates)

    DebugPrinter.println("\n*** second path running ***")
    val startJob2 = System.currentTimeMillis()
    ReduceAlgo2Job2.run(outputTmp, output, reduceClassName, deflates)

    val conf = new Configuration()
    conf.set("reduceClassName", reduceClassName)
    val fs = output.getFileSystem(conf)
    val paths = fs.listStatus(output, new RegexPathFilter(".*part-r-.*")).map(_.getPath).toList

    DebugPrinter.println("\n*** last phase running ***")
    val startLast = System.currentTimeMillis()
    val ans = Consummate.exec(paths, conf)

    val end = System.currentTimeMillis()
    println("answer: " + ans)
    println("Job1: " + ((startJob2 - startJob1) / 1000) + " sec")
    println("Job2: " + ((startLast - startJob2) / 1000) + " sec")
    println("LastReduce: " + ((end - startLast) / 1000) + " sec")
    println("Total: " + ((end - startJob1) / 1000) + " sec")
  }
}

object ReduceAlgo2Job1 {
  def run(input: Path, outputTmp: Path, reduceClassName: String, deflates: Boolean) {
    val conf = new org.apache.hadoop.mapred.JobConf()
    conf.set("reduceClassName", reduceClassName)
    conf.set("outputTmp", outputTmp.getName)
    conf.set("outputTmpFull", outputTmp.toString)
    conf.setInt("mapred.reduce.tasks", 1) // important

    if (deflates) {
      org.apache.hadoop.mapred.SequenceFileOutputFormat.setOutputCompressionType(
        conf, SequenceFile.CompressionType.BLOCK)
    }

    // for output in Map task
    MultipleOutputs.addNamedOutput(conf, "hNF",
                                   classOf[org.apache.hadoop.mapred.SequenceFileOutputFormat[NullWritable, Text]],
                                   classOf[NullWritable],
                                   classOf[Text])

    conf.setJarByClass(Class.forName("TreeReduction"))
    conf.setJobName(input.getName + ": algo2, pass1")

    org.apache.hadoop.mapred.FileInputFormat.addInputPath(conf, input)
    org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(conf, outputTmp)

    conf.setMapperClass(classOf[ReduceAlgo2Map1])
    conf.setMapOutputKeyClass(classOf[NullWritable])
    conf.setMapOutputValueClass(classOf[HNFShapeWritable])

    conf.setReducerClass(classOf[ReduceAlgo2Reduce1])
    conf.setOutputKeyClass(classOf[NullWritable])
    conf.setOutputValueClass(classOf[NullWritable])

    org.apache.hadoop.mapred.JobClient.runJob(conf)
  }
}

object ReduceAlgo2Job2 {
  def run(outputTmp: Path, output: Path, reduceClassName: String, deflates: Boolean) {
    val offsetDepthMap = new Path(outputTmp, "offsetDepthMap")
    val matingPairsLeftFile = new Path(outputTmp, "matingPairsLeft")
    val matingPairsRightFile = new Path(outputTmp, "matingPairsRight")

    val conf = new Configuration()
    conf.set("reduceClassName", reduceClassName)
    conf.set("offsetDepthMapFile", offsetDepthMap.toString)
    conf.set("matingPairsLeftFile", matingPairsLeftFile.toString)
    conf.set("matingPairsRightFile", matingPairsRightFile.toString)

    val job = new Job(conf)
    job.setJarByClass(Class.forName("TreeReduction"))
    job.setJobName(output.getName + ": algo2, pass2")
    job.setInputFormatClass(classOf[SequenceFileInputFormat[NullWritable, Text]])

    val fs = outputTmp.getFileSystem(conf)
    val paths = fs.listStatus(outputTmp, new RegexPathFilter(".*hNF.*")).map(_.getPath)
    val pathsWithComma = paths.map(new Path(outputTmp, _)).mkString(",")
    FileInputFormat.addInputPaths(job, pathsWithComma)

    FileOutputFormat.setOutputPath(job, output)

    job.setMapperClass(classOf[ReduceAlgo2Map2])
    job.setReducerClass(classOf[ReduceAlgo2Reduce2])

    job.setMapOutputKeyClass(classOf[MatingPairWritable])
    job.setMapOutputValueClass(classOf[Text])

    job.setOutputKeyClass(classOf[MatingPairWritable])
    job.setOutputValueClass(classOf[AbstractReduce#TripletWritable])

    job.waitForCompletion(true)
  }
}
