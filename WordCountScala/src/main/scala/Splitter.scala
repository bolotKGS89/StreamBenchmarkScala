package wordCount

import org.apache.spark.streaming.dstream.DStream
import Util.Log
import org.apache.spark

class Splitter(lines: DStream[String]) {

    def execute(): DStream[String] ={
        var timestamp: Long = 0
        var bytes: Long = 0
        var lineCount: Long = 0
        var tStart: Long = 0
        var tEnd: Long = 0
        var wordCount: Long = 0

        lines.transform({ rdd =>

          val words = rdd.filter((line) => !line.isEmpty)
          .flatMap((line) => {
              bytes += line.getBytes.length
              lineCount += 1
              line.split(" ")
          }).map((words) => {
              wordCount += 1
              words
          })


          tStart = System.nanoTime
          words
        }).map((words) => {
          tEnd = System.nanoTime
          val tElapsed = (tEnd - tStart) / 1000000 // elapsed time in milliseconds

          val mbs = (bytes / 1048576).toDouble / (tElapsed / 1000).toDouble
          val formatted_mbs = String.format("%.5f", mbs)

//          Log.log.warn(s"Measured throughput: ${formatted_mbs} MB/second")

//          Log.log.info("[Splitter] execution time: " + tElapsed + " ms, " +
//                 "processed: " + lineCount + " (lines) "
//                               + wordCount + " (words) "
//                               + (bytes / 1048576) + " (MB), " +
//                 "bandwidth: " + wordCount / (tElapsed / 1000) + " (words/s) "
//                               + formatted_mbs + " (MB/s) "
//                               + bytes / (tElapsed / 1000) + " (bytes/s)");


          words
        })
    }
}
