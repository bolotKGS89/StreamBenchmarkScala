package WordCount

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import Util.{Log}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.TaskContext

class Splitter(lines: DStream[(String, Long)], ssc: StreamingContext, parDegree: Int) {

    def execute(): DStream[(String, Int, Long)] ={
        var lineCount: Long = 0
        var wordCount: Long = 0
        var timestamp: Long = 0L

        val counter = ssc.sparkContext.longAccumulator("Splitter accumulator")

        lines.transform({ rdd =>
//            val taskContext = TaskContext.get
          val startTime = System.nanoTime()
//             val startMetrics = taskMetrics

          val words = rdd.repartition(parDegree).filter((data) => !data._1.isEmpty)
          .flatMap((data) => {
            counter.add(data._1.getBytes.length)
            lineCount += 1
            val words = data._1.split(" ")
            timestamp = data._2
            words
          }).map((word) => {
            wordCount += 1
            (word, 1)
          }).reduceByKey(_ + _).map((wordTuple) => (wordTuple._1, wordTuple._2, timestamp))

//            val endMetrics = taskMetrics
//            val latency = taskContext.taskMetrics.executorRunTime
//            val inputBytes = endMetrics.inputMetrics.bytesRead - startMetrics.inputMetrics.bytesRead
//            val outputBytes = endMetrics.outputMetrics.bytesWritten - startMetrics.outputMetrics.bytesWritten
//            val numBytes = outputBytes - inputBytes
//
//            val duration = (endTime - startTime) / 1000.0
//            val bandwidth = numBytes / duration
//            Log.log.warn(s"[Splitter] bandwidth: $bandwidth MB/s")

          val endTime = System.nanoTime()
          val latency = endTime - startTime // Measure the time it took to process the data
          Log.log.warn(s"[Splitter] latency: $latency")

          val elapsedTime = (endTime - startTime) / 1000000000.0
          val mbs: Double = (counter.sum / elapsedTime).toDouble
//          val formattedMbs = String.format("%.5f", mbs)
//          Log.log.warn(s"[Splitter] bandwidth: $formattedMbs MB/s")

          words
        })

    }
}
