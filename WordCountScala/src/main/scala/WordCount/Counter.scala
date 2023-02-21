package WordCount

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import Util.{Log, MetricsCollector, Sampler}


class Counter(words: DStream[(String, Int, Long)], ssc: StreamingContext, samplingRate: Long, parDegree: Int) {

  Sampler.init(samplingRate)
  def count(): DStream[(String, Int)] = {
    val counter = ssc.sparkContext.longAccumulator("Counter accumulator")
    words.transform({ rdd =>
      val startTime = System.nanoTime()

      val res = rdd.repartition(parDegree).map(wordTuple => { // might be wrong use
          val word = wordTuple._1
          val count = wordTuple._2
          val timestamp = wordTuple._3

          counter.add(word.getBytes.length)

          val now = System.nanoTime
          Sampler.add((now - timestamp).toDouble / 1e3, now)

          System.out.println((word, count))

          (word, count)
      })

      val endTime = System.nanoTime()
      val latency = endTime - startTime // Measure the time it took to process the data
      Log.log.info(s"[Counter] latency: $latency")

      val elapsedTime = (endTime - startTime) / 1000000000.0
      val mbs: Double = (counter.sum / elapsedTime).toDouble
      val formattedMbs = String.format("%.5f", mbs)
      Log.log.info(s"[Counter] bandwidth: $formattedMbs MB/s")

      res
    })
  }
}
