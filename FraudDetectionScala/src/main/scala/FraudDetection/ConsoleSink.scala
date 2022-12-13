package FraudDetection

import Util.Sampler
import org.apache.spark.streaming.dstream.DStream

class ConsoleSink(samplingRate: Long) {
    private val latency = Sampler(samplingRate)
    private var processed = 0
    private var tStart = 0L
    private var tEnd = 0L
    def print(filteredTuples: DStream[(String, Double, String, Long)]): Unit = {
      tStart = System.nanoTime

      filteredTuples.transform({ rdd =>
        val res = rdd.map((tuple) => {
          val entityId: String = tuple._1
          val score = tuple._2
          val states = tuple._3
          val timestamp = tuple._4
          val now = System.nanoTime
          latency.add((now - timestamp).toDouble / 1e3, now)
          processed += 1
        })

        res
        //      val t_elapsed = (tEnd - tStart) / 1000000 // elapsed time in milliseconds

      })

    }
}
