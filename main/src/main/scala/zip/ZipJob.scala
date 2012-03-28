 // common
import org.apache.hadoop.io._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
 // Job2
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

class ZipJob(input1: Path, input2: Path, outputTmp: Path, output: Path, zipClassName: String, deflates: Boolean) {
  def run() {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    run1(input1, 1)
    run1(input2, 2)
    run2(input1, 1)
    run2(input2, 2)
    run3()
  }

  private def run3() {
    val conf = new Configuration()
    conf.set("zipClassName", zipClassName)
    val fs = outputTmp.getFileSystem(conf)
    def getPathsWithComma(inputType: Int) = {
      val elms = new Path(outputTmp, "elms" + inputType)
      val paths = fs.listStatus(elms, new RegexPathFilter(".*part-m-.*")).map(_.getPath)
      paths.map(new Path(elms, _)).mkString(",")
    }

    val job = new Job(conf)
    FileInputFormat.addInputPaths(job, getPathsWithComma(1))
    FileInputFormat.addInputPaths(job, getPathsWithComma(2))

    FileOutputFormat.setOutputPath(job, output)

    job.setMapperClass(classOf[Mapper])
    job.setReducerClass(classOf[ZipReduce3])

    job.setMapOutputKeyClass(classOf[NumElmOptionWritable])
    job.setMapOutputValueClass(classOf[Text])

    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[Text])

    job.waitForCompletion(true)
  }

  private def run1(input: Path, inputType: Int) {
    val offsetNumElmMap = new Path(outputTmp, "offsetNumElmMap" + inputType)

    val conf = new Configuration()
    conf.set("zipClassName", zipClassName)
    conf.setInt("zipInputType", inputType)
    conf.set("offsetNumElmMap", offsetNumElmMap.toString)

    val job = new Job(conf)
    job.setJarByClass(Class.forName("TreeReduction"))
    job.setJobName(input.getName + ": zip, pass1")

    FileInputFormat.addInputPaths(job, input.toString)
    FileOutputFormat.setOutputPath(job, new Path(outputTmp, "discarded"))

    job.setMapperClass(classOf[ZipMap1])
    job.setReducerClass(classOf[ZipReduce1])

    job.setMapOutputKeyClass(classOf[NullWritable])
    job.setMapOutputValueClass(classOf[NumElmWritable])

    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[NullWritable])

    job.waitForCompletion(true)

    fs.delete(new Path(outputTmp, "discarded"), true)
  }

  private def run2(input: Path, inputType: Int) {
    val offsetNumElmMap = new Path(outputTmp, "offsetNumElmMap" + inputType)

    val conf = new Configuration()
    conf.set("zipClassName", zipClassName)
    conf.setInt("zipInputType", inputType)
    conf.set("offsetNumElmMap", offsetNumElmMap.toString)

    val job = new Job(conf)
    job.setJarByClass(Class.forName("TreeReduction"))
    job.setJobName(input.getName + ": zip, pass1")

    FileInputFormat.addInputPaths(job, input.toString)
    FileOutputFormat.setOutputPath(job, new Path(outputTmp, "elms" + inputType))

    job.setMapperClass(classOf[ZipMap2])
    job.setNumReduceTasks(0)

    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[Text])

    job.waitForCompletion(true)
  }
}
