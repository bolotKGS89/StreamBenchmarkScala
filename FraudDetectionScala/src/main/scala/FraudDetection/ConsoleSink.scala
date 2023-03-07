package FraudDetection

import Util.{Log, Sampler}
import org.apache.spark.streaming.dstream.DStream

class ConsoleSink(samplingRate: Long) {

    def print(filteredTuples: DStream[(String, Double, String, Long)], sinkParDeg: Int): DStream[(String, Double, String, Long)] = {

      filteredTuples.transform({ rdd =>
        val startTime = System.nanoTime()
        val res = rdd.repartition(sinkParDeg).map({ case(entityId, score, states, timestamp) => {
          (entityId, score, states, timestamp)
        }})

        val endTime = System.nanoTime()
        val latency = endTime - startTime // Measure the time it took to process the data
        Log.log.warn(s"[Predictor] latency: $latency")

        res
      })

    }
}
