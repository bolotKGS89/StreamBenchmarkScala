package WordCount

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import Util.{Log, MetricsCollector, Sampler}


class Counter(words: DStream[(String, Int, Long)], ssc: StreamingContext, samplingRate: Long, parDegree: Int) {

//  Sampler.init(samplingRate)
  def count(): DStream[(String, Int)] = {
    val counter = ssc.sparkContext.longAccumulator("Counter accumulator")
    words.transform({ rdd =>
//            val taskContext = TaskContext.get
      val startTime = System.nanoTime()
      val sampler = new Sampler(samplingRate)

      val res = rdd.repartition(parDegree).map(wordTuple => { // might be wrong use
          val word = wordTuple._1
          val count = wordTuple._2
          val timestamp = wordTuple._3

          counter.add(word.getBytes.length)

          val now = System.nanoTime
          sampler.add((now - timestamp).toDouble / 1e3, now)


          (word, count)
      })

      val endTime = System.nanoTime()
      val latency = endTime - startTime // Measure the time it took to process the data
      Log.log.info(s"[Counter] latency: $latency")

      val elapsedTime = (endTime - startTime) / 1000000000.0
      val mbs: Double = (counter.sum / elapsedTime).toDouble
      val formattedMbs = String.format("%.5f", mbs)
      Log.log.info(s"[Counter] bandwidth: $formattedMbs MB/s")

      //            val endMetrics = taskMetrics
      //            val latency = taskContext.taskMetrics.executorRunTime
      //            val inputBytes = endMetrics.inputMetrics.bytesRead - startMetrics.inputMetrics.bytesRead
      //            val outputBytes = endMetrics.outputMetrics.bytesWritten - startMetrics.outputMetrics.bytesWritten
      //            val numBytes = outputBytes - inputBytes
      //
      //            val duration = (endTime - startTime) / 1000.0
      //            val bandwidth = numBytes / duration
      //            Log.log.warn(s"[Splitter] bandwidth: $bandwidth MB/s")


      res
    })
  }
}
