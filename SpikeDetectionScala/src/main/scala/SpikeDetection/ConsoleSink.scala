package SpikeDetection

import Util.Sampler
import org.apache.spark.streaming.dstream.DStream

class ConsoleSink(samplingRate: Long) {
  private var tStart = 0L
  private var tEnd = 0L

  def print(filteredTuples: DStream[(String, Double, Double, Long)], sinkParDeg: Int): DStream[(String, Double, Double, Long)] = {
    tStart = System.nanoTime
    val latency = new Sampler(samplingRate)
    var processed = 0

    filteredTuples.transform({ rdd =>
        val res = rdd.repartition(sinkParDeg).map((tuple) => {
          val deviceId: String = tuple._1
          val movingAvgInstant = tuple._2
          val nextPropertyValue = tuple._3
          val timestamp = tuple._4
          val now = System.nanoTime
          latency.add((now - timestamp).toDouble / 1e3, now)
          processed += 1
          (deviceId, movingAvgInstant, nextPropertyValue, timestamp)
        })

        res
//      val t_elapsed = (tEnd - tStart) / 1000000 // elapsed time in milliseconds

    })
  }
}
