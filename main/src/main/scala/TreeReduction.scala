import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import org.apache.hadoop.conf.Configuration

object TreeReduction {

  def main(args : Array[String]){
    var valueClassName = ""
    var isMode1 = true // mode 1 is default
    var deflates = false

    def getopt(i: Int): Int = {
      args(i) match {
        case "-value" => {
          valueClassName = args(i + 1)
          getopt(i + 2)
        }
        case "-mode" => {
          isMode1 = if(args(i + 1) == "1") true
                    else if(args(i + 1) == "2") false
                    else throw new IllegalArgumentException("mode option must be 1 or 2")
          getopt(i + 2)}
        case "-c" => {
          deflates = true
          getopt(i + 1)}
        case _ => i
      }
    }

    val index = getopt(0)

    if(args.length - index != 3){
      println("Usage: hadoop jar treereduction.jar [-mode 1 | 2] [-c] <input path> <output path> <log path>")
      System.exit(-1)
    }

    // path setting
    val input = new Path(args(index))
    val output = new Path(args(index + 1))
    val outputTmp = new Path(args(index + 1) + "Tmp")

    // setting log
    val logdir = new Path(args(index + 2))

    println("mode: " + (if(isMode1) "1" else "2"))
    println("deflates: " + (if(deflates) "on" else "off"))
    println("input: " + input)
    println("output: " + output)
    println("logdir: " + logdir)

    if(isMode1) {
      Algo1Job.run(input, output, valueClassName, deflates)
    } else {
      Algo2Job.run(input, outputTmp, output, valueClassName, deflates)
    }
  }
}

