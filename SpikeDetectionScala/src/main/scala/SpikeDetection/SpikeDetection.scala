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
      var processed = 0
        rdd.map({ case (deviceId, movingAvgInstant, nextPropertyValue, timestamp) =>
          processed += 1
//          if (processed <= 20) System.out.println(deviceId + " " + movingAvgInstant + " " + nextPropertyValue + " " + timestamp)

          (deviceId, movingAvgInstant, nextPropertyValue, timestamp)
        }).filter({ case (_, movingAvgInstant, nextPropertyValue, _) =>
          Math.abs(nextPropertyValue - movingAvgInstant) > spikeThreshold * movingAvgInstant // fix it
        }).map { case (deviceId, movingAvgInstant, nextPropertyValue, timestamp) =>
          spikes += 1
//          if (spikes <= 20) System.out.println(deviceId + " " + movingAvgInstant + " " + nextPropertyValue + " " + timestamp)
          (deviceId, movingAvgInstant, movingAvgInstant, timestamp)
        }.repartition(counterParDeg)
    })
  }
}
