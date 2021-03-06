import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object ReduceAlgo1Job {
  def run(input: Path, output: Path, reduceClassName: String, deflates: Boolean) {
    DebugPrinter.println("**** mode 1 ****")
    val start = System.currentTimeMillis()

    val conf = new Configuration()
    conf.set("reduceClassName", reduceClassName)
    conf.setInt("mapred.reduce.tasks", 1)

    val job = new Job(conf)
    job.setJarByClass(Class.forName("TreeReduction"))
    job.setJobName(input.getName + ": algo1")

    FileOutputFormat.setCompressOutput(job, deflates)
    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)

    job.setMapperClass(classOf[ReduceAlgo1Map])
    job.setReducerClass(classOf[ReduceAlgo1Reduce])

    job.setMapOutputKeyClass(classOf[NullWritable])
    job.setMapOutputValueClass(classOf[Text])

    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])

    job.waitForCompletion(true)

    val end = System.currentTimeMillis()
    println("Total: " + ((end - start) / 1000) + "sec")

  }

}
