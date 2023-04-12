package SpikeDetection

import Util.Log
import org.apache.spark.streaming.dstream.DStream

import java.util.Properties

class SpikeDetection extends Serializable {
  private var spikes = 0
  private var processed = 0

  def execute(tuples: DStream[(String, Double, Double, Long)], counterParDeg: Int, spikeThreshold: Double):
  DStream[(String, Double, Double, Long)] = {

    tuples.transform({ rdd =>
      val startTime = System.nanoTime()
        rdd.repartition(counterParDeg).map({ case (deviceId, movingAvgInstant, nextPropertyValue, timestamp) =>
          if (Math.abs(nextPropertyValue - movingAvgInstant) > spikeThreshold * movingAvgInstant) {
            spikes += 1

            (deviceId, movingAvgInstant, movingAvgInstant, timestamp)
          } else {

            null
          }
        }).filter(_ != null)
    })
  }
}
