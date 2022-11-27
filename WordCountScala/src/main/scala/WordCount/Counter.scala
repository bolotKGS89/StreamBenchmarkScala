package WordCount

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import Util.Sampler
import Util.MetricsCollector


class Counter(words: DStream[(String, Int, Long)], ssc: StreamingContext, samplingRate: Long) {

  Sampler.init(samplingRate)
  def count(): DStream[(String, Int)] = {
    words.transform { rdd =>

      val counter = ssc.sparkContext.collectionAccumulator[String]
      val metrics = new MetricsCollector
      val res = rdd.map(wordTuple => {
          val word = wordTuple._1
          val count = wordTuple._2
          val timestamp = wordTuple._3

          val now = System.nanoTime
          Sampler.add((now - timestamp).toDouble / 1e3, now)
          metrics.collectMetrics(word, counter)

          (word, count)
      })
      res
    }
  }
}
