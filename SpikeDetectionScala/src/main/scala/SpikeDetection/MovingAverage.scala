package SpikeDetection

import Util.Log
import org.apache.spark.streaming.dstream.DStream

import java.util

class MovingAverage extends Serializable {

  private var processed = 0
  private var tStart = 0L
  private var tEnd = 0L

  def execute(tuples: DStream[(String, Double, Long)]): DStream[(String, Double, Double, Long)] = {

    tuples.transform({ rdd =>
      tStart = System.nanoTime

      val res = rdd.map((tuple) => {
        val deviceId = tuple._1
        val nextPropertyValue = tuple._2.toDouble
        val timestamp = tuple._3

        val movingAverageInstant = movingAverage(deviceId, nextPropertyValue)
        processed += 1
        Log.log.debug("[Average] tuple: deviceID "
          + deviceId + ", incremental_average "
          + movingAverageInstant + ", next_value "
          + nextPropertyValue + ", ts " + timestamp)

//        tEnd = System.nanoTime

        (deviceId, movingAverageInstant, nextPropertyValue, timestamp)
      }) // ending should be done
      res
    })

    //val tElapsed = (tEnd - tStart) / 1000000 // elapsed time in milliseconds
    /*LOG.info("[Average] execution time: " + t_elapsed +
                " ms, processed: " + processed +
                ", bandwidth: " + processed / (t_elapsed / 1000) +  // tuples per second
                " tuples/s");*/

  }


  private def movingAverage(deviceId: String, nextDouble: Double): Double = {
    var valueList: util.LinkedList[Double] = new util.LinkedList[Double]
    val deviceIDtoStreamMap: util.Map[String, util.LinkedList[Double]] = new util.HashMap[String, util.LinkedList[Double]]
    val deviceIDtoSumOfEvents: util.Map[String, Double] = new util.HashMap[String, Double]
    var sum: Double = 0.0
    var movingAverageWindow = 1000

    if (deviceIDtoStreamMap.containsKey(deviceId)) {
      valueList = deviceIDtoStreamMap.get(deviceId)
      sum = deviceIDtoSumOfEvents.get(deviceId)
      if (valueList.size > movingAverageWindow - 1) {
        val valueToRemove: Double = valueList.removeFirst
        sum -= valueToRemove
      }
      valueList.addLast(nextDouble)
      sum += nextDouble
      deviceIDtoSumOfEvents.put(deviceId, sum)
      deviceIDtoStreamMap.put(deviceId, valueList)
      return sum / valueList.size
    }  else {
      valueList.add(nextDouble)
      deviceIDtoStreamMap.put(deviceId, valueList)
      deviceIDtoSumOfEvents.put(deviceId, nextDouble)
      return nextDouble
    }

  }
}
