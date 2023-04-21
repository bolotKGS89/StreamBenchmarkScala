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
        val res = rdd.map{ case(deviceId, movingAvgInstant, nextPropertyValue, timestamp) =>
          val now = System.nanoTime
          latency.add((now - timestamp).toDouble / 1e3, now)
          processed += 1
//          if (processed <= 20)
//            System.out.println("deviceID " + deviceId + " moving_avg_instant " + movingAvgInstant + " next_property_value " + nextPropertyValue)

          (deviceId, movingAvgInstant, nextPropertyValue, timestamp)
        }.repartition(sinkParDeg)

        res
//      val t_elapsed = (tEnd - tStart) / 1000000 // elapsed time in milliseconds

    })
  }
}
