package WordCount

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import Util.MetricsCollector

class Splitter(lines: DStream[(String, Long)], ssc: StreamingContext) {

    def execute(): DStream[(String, Int, Long)] ={
        var lineCount: Long = 0
        var wordCount: Long = 0
        var timestamp: Long = 0L

        lines.transform({ rdd =>
          val counter = ssc.sparkContext.collectionAccumulator[String]
          val metrics = new MetricsCollector
          val words = rdd.filter((data) => !data._1.isEmpty)
          .flatMap((data) => {
            metrics.collectMetrics(data._1, counter)
            lineCount += 1
            val words = data._1.split(" ")
            timestamp = data._2
            words
          }).map((word) => {
            wordCount += 1
            (word, 1)
          }).reduceByKey(_ + _).map((wordTuple) => (wordTuple._1, wordTuple._2, timestamp))

          words
        })

    }
}

//          Log.log.warn(s"Measured throughput: ${formatted_mbs} MB/second")

//          Log.log.info("[Splitter] execution time: " + tElapsed + " ms, " +
//                 "processed: " + lineCount + " (lines) "
//                               + wordCount + " (words) "
//                               + (bytes / 1048576) + " (MB), " +
//                 "bandwidth: " + wordCount / (tElapsed / 1000) + " (words/s) "
//                               + formatted_mbs + " (MB/s) "
//                               + bytes / (tElapsed / 1000) + " (bytes/s)");
