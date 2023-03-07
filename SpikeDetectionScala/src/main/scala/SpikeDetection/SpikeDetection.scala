package SpikeDetection

import Util.Log
import org.apache.spark.streaming.dstream.DStream

class SpikeDetection extends Serializable {
  private var spikes = 0
  private var processed = 0
  private val spikeThreshold = 0.01d
  def execute(tuples: DStream[(String, Double, Double, Long)], counterParDeg: Int): DStream[(String, Double, Double, Long)] = {

    tuples.transform({ rdd =>
      val startTime = System.nanoTime()

      val ans = rdd.repartition(counterParDeg).filter({ case(deviceId, movingAvgInstant, nextPropertyValue, timestamp) => {

        Log.log.debug("[Detector] tuple: deviceID " + deviceId +
          ", incremental_average " + movingAvgInstant +
          ", next_value " + nextPropertyValue +
          ", ts " + timestamp)
        processed += 1

        Math.abs(nextPropertyValue - movingAvgInstant) > spikeThreshold * movingAvgInstant
      }}).map({ case(deviceId, movingAvgInstant, nextPropertyValue, timestamp) => {
        spikes += 1

        (deviceId, movingAvgInstant, nextPropertyValue, timestamp)
      }})
      val endTime = System.nanoTime
      val latency = endTime - startTime // Measure the time it took to process the data
      Log.log.warn(s"[SpikeDetection] latency: $latency")
      // bandwidth

      ans
    })
  }
}
