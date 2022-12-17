package FraudDetection

import Util.Sampler
import org.apache.spark.streaming.dstream.DStream

class ConsoleSink(samplingRate: Long) {
    private var tStart = 0L
    private var tEnd = 0L
    def print(filteredTuples: DStream[(String, Double, String, Long)]): DStream[(String, Double, String, Long)] = {
      tStart = System.nanoTime
      val latency = Sampler(samplingRate)
      var processed = 0

      filteredTuples.transform({ rdd =>
        val res = rdd.map((tuple) => {
          val entityId: String = tuple._1
          val score = tuple._2
          val states = tuple._3
          val timestamp = tuple._4
          val now = System.nanoTime
          latency.add((now - timestamp).toDouble / 1e3, now)
          processed += 1
          (entityId, score, states, timestamp)
        })

        res
        //      val t_elapsed = (tEnd - tStart) / 1000000 // elapsed time in milliseconds

      })

    }
}
