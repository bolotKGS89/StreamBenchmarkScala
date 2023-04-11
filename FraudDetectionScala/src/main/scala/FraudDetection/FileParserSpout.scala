package FraudDetection

import Util.Log
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io.FileNotFoundException
import scala.collection.mutable.Queue

class FileParserSpout(path: String, ssc: StreamingContext) {

  def parseDataSet(splitRegex: String, sourceParDeg: Int): DStream[(String, String, Long)] = {

    try {
      val counter = ssc.sparkContext.longAccumulator("Splitter accumulator")
      val rdd = ssc.sparkContext.textFile(path)

      ssc.queueStream(
        Queue(rdd)
      ).transform({ rdd =>
        val startTime = System.nanoTime()
        val res = rdd.repartition(sourceParDeg).map(line => {
          val splitLines = line.split(splitRegex, 2)
          counter.add(line.getBytes.length)
          val timestamp = System.nanoTime
          (splitLines(0).trim, splitLines(1).trim, timestamp)
        })

        val endTime = System.nanoTime()
        val latency = endTime - startTime // Measure the time it took to process the data
        Log.log.warn(s"[Source] latency: $latency")

        val elapsedTime = (endTime - startTime) / 1000000000.0
        val mbs: Double = (counter.sum / elapsedTime).toDouble
//        val formattedMbs = String.format("%.5f", mbs)
//        Log.log.warn(s"[Source] bandwidth: $formattedMbs MB/s")

        res
      })

    } catch {
      case _: FileNotFoundException | _: NullPointerException => {
        Log.log.error(s"The file {} does not exists $path")
        throw new RuntimeException(s"The file $path does not exists")
      }
    }

  }
   private def activeDelay(nsecs: Double): Unit  = {
       val t_start = System.nanoTime
       var t_now = 0L
       var end = false
       while (!end) {
         t_now = System.nanoTime
         end = (t_now - t_start) >= nsecs
       }
  }
}
