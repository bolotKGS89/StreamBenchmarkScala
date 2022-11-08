package spikeDetection
import org.apache.spark.streaming.dstream.DStream

class SpikeDetection {
  def execute(tuples: DStream[(String, Double, Double, Long)]): DStream[(String, Double, Double, Long)] = {
    val spikeThreshold = 0.25d
    var processed = 0
    tuples.filter((tuple) => {
      val deviceId: String = tuple._1
      val movingAvgInstant = tuple._2
      val nextPropertyValue = tuple._3
      val timestamp = tuple._4

      val ans = Math.abs(nextPropertyValue - movingAvgInstant) > spikeThreshold * movingAvgInstant
      if(ans)
        processed += 1

      ans
    })
  }
}
