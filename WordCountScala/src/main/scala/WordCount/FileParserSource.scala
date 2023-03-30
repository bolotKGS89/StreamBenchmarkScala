package WordCount

import Util.Log
import org.apache.spark.TaskContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.executor.TaskMetrics

import java.io.FileNotFoundException
import scala.collection.mutable.Queue

class FileParserSource(path: String, ssc: StreamingContext, parDegree: Int) {

  def parseDataSet(): DStream[(String, Long)] = {
    var timestamp = 0L
    try {
          val counter = ssc.sparkContext.longAccumulator
          val rdd = ssc.sparkContext.textFile(path)

          ssc.queueStream(
            Queue(rdd)
          ).transform({ rdd =>
//            val taskContext = TaskContext.get
            val startTime = System.nanoTime()
//             val startMetrics = taskMetrics

             val res = rdd.repartition(parDegree).flatMap((line) => {
             val splitLine = line.split("\n").filter((line) => !line.isEmpty)
             timestamp = System.nanoTime
             counter.add(line.getBytes.length)
             splitLine })
            .map((line) => (line, timestamp))

             val endTime = System.nanoTime()
//             val endMetrics = taskMetrics
//             val latency = taskContext.taskMetrics.executorRunTime
             val latency = endTime - startTime // Measure the time it took to process the data
             Log.log.warn(s"[Source] latency: $latency")

//            val inputBytes = endMetrics.inputMetrics.bytesRead - startMetrics.inputMetrics.bytesRead
//            val outputBytes = endMetrics.outputMetrics.bytesWritten - startMetrics.outputMetrics.bytesWritten
//            val numBytes = outputBytes - inputBytes
//
//            val duration = (endTime - startTime) / 1000.0
//            val bandwidth = numBytes / duration
//            Log.log.warn(s"[Source] bandwidth: $bandwidth MB/s")

             val elapsedTime = (endTime - startTime) / 1000000000.0
             val mbs: Double = (counter.sum / elapsedTime).toDouble
//             val formatted_mbs = String.format("%.5f", mbs)
//             Log.log.warn(s"[Source] bandwidth: $formatted_mbs MB/s")

             res
        })

    } catch {
      case _: FileNotFoundException | _: NullPointerException => {
        Log.log.error(s"The file {} does not exists $path")
        throw new RuntimeException(s"The file $path does not exists")
      }
    }
  }
}
