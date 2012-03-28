import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import org.apache.hadoop.conf.Configuration

object TreeReduction {

  def main(args : Array[String]){
    if(args.length == 0){
      println("Usage: hadoop jar treereduction.jar [-c] [-reduce1 | -reduce2 | zip] <class name> <input paths> <output path>")
      System.exit(-1)
    }

    var deflates = false

    def getopt(i: Int) {
      args(i) match {
        case "-reduce1" =>
          val reduceClassName = args(i + 1)
          val input = new Path(args(i + 2))
          val output = new Path(args(i + 3))
          val outputTmp = new Path(args(i + 3) + "Tmp")
          ReduceAlgo1Job.run(input, output, reduceClassName, deflates)
        case "-reduce2" =>
          val reduceClassName = args(i + 1)
          val input = new Path(args(i + 2))
          val output = new Path(args(i + 3))
          val outputTmp = new Path(args(i + 3) + "Tmp")
          ReduceAlgo2Job.run(input, outputTmp, output, reduceClassName, deflates)
        case "-zip" =>
          val zipClassName = args(i + 1)
          val input1 = new Path(args(i + 2))
          val input2 = new Path(args(i + 3))
          val output = new Path(args(i + 4))
          val outputTmp = new Path(args(i + 4) + "Tmp")
          val job = new ZipJob(input1, input2, outputTmp, output, zipClassName, deflates)
          job.run()
        case "-c" =>
          deflates = true
          getopt(i + 1)
        case s =>
          println("unknown option: " + s)
          System.exit(-1)
      }
    }

    getopt(0)
  }
}

