package SpikeDetection

import org.apache.spark.streaming.dstream.DStream

import java.util

class MovingAverage {

  def execute(tuples: DStream[(String, String, Long)]) = {
    var valueList: util.LinkedList[Double] = new util.LinkedList[Double]
    val deviceIDtoStreamMap: util.Map[String, util.LinkedList[Double]] = new util.HashMap[String, util.LinkedList[Double]]
    val deviceIDtoSumOfEvents: util.Map[String, Double] = new util.HashMap[String, Double]
    var sum: Double = 0.0
    var movingAverageWindow = 1000

    tuples.map((tuple) => {
      val deviceId = tuple._1
      val value = tuple._2.toDouble
      val timestamp = tuple._3

      if (deviceIDtoStreamMap.containsKey(deviceId)) {
        valueList = deviceIDtoStreamMap.get(deviceId)
        sum = deviceIDtoSumOfEvents.get(deviceId)
        if (valueList.size > movingAverageWindow - 1) {
          val valueToRemove: Double = valueList.removeFirst
          sum -= valueToRemove
        }
        valueList.addLast(value)
        sum += value
        deviceIDtoSumOfEvents.put(deviceId, sum)
        deviceIDtoStreamMap.put(deviceId, valueList)
        (deviceId, sum / valueList.size, value, timestamp)
      }
      else {
        valueList.add(value)
        deviceIDtoStreamMap.put(deviceId, valueList)
        deviceIDtoSumOfEvents.put(deviceId, value)
        (deviceId, value, value, timestamp)
      }
    })
  }
}
