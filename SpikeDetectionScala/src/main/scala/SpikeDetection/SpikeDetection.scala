package SpikeDetection

import Util.Log
import org.apache.spark.streaming.dstream.DStream

class SpikeDetection extends Serializable {
  private var spikes = 0
  private var processed = 0
  private val spikeThreshold = 0.01d
  def execute(tuples: DStream[(String, Double, Double, Long)]): DStream[(String, Double, Double, Long)] = {

    tuples.transform({ rdd =>

      val ans = rdd.filter((tuple) => {
        val deviceId: String = tuple._1
        val movingAvgInstant = tuple._2
        val nextPropertyValue = tuple._3
        val timestamp = tuple._4

        Log.log.debug("[Detector] tuple: deviceID " + deviceId +
          ", incremental_average " + movingAvgInstant +
          ", next_value " + nextPropertyValue +
          ", ts " + timestamp)
        processed += 1

        Math.abs(nextPropertyValue - movingAvgInstant) > spikeThreshold * movingAvgInstant
      }).map((tuple) => {
        spikes += 1
        val deviceId: String = tuple._1
        val movingAvgInstant = tuple._2
        val nextPropertyValue = tuple._3
        val timestamp = tuple._4
        (deviceId, movingAvgInstant, nextPropertyValue, timestamp)
      })
      ans
    })
  }
}
