package TrafficMonitoring

import RoadModel.Road
import Util.Log
import org.apache.spark.streaming.dstream.DStream

import java.util

class SpeedCalculator(lines: DStream[(Int, Int, Long)], parDegree: Int) extends Serializable {
    def execute(): DStream[(Int, Int, Int, Long)] = {
        var roads: util.Map[Integer, Road] = new util.HashMap[Integer, Road];
        var averageSpeed = 0
        var count = 0

        lines.transform({ rdd =>
            val startTime = System.nanoTime()
            val counter = rdd.sparkContext.longAccumulator

            val res = rdd.repartition(parDegree).map({ case (roadID, speed, timestamp) => {
                Log.log.debug("[Calculator] tuple: roadID " + roadID + ", speed " + speed + ", ts " + timestamp)

                if (!roads.containsKey(roadID)) {
                    val road: Road = new Road(roadID)
                    road.addRoadSpeed(speed)
                    road.setCount(1)
                    road.setAverageSpeed(speed)

                    roads.put(roadID, road)
                    averageSpeed = speed
                    count = 1
                } else {
                    val road: Road = roads.get(roadID)

                    var sum: Int = 0

                    if (road.getRoadSpeedSize() < 2) {
                        road.incrementCount()
                        road.addRoadSpeed(speed)

                        road.getRoadSpeed().forEach((it) => sum += it)


                        averageSpeed = (sum.toDouble / road.getRoadSpeedSize().toDouble).toInt
                        road.setAverageSpeed(averageSpeed)
                        count = road.getRoadSpeedSize()
                    } else {
                        val avgLast: Double = roads.get(roadID).getAverageSpeed()
                        var temp: Double = 0


                        road.getRoadSpeed().forEach((it) => {
                            sum += it
                            temp += Math.pow((it - avgLast), 2)
                        })

                        val avgCurrent: Int = ((sum + speed) / (road.getRoadSpeedSize() + 1).toDouble).toInt
                        temp = (temp + Math.pow((speed - avgLast), 2)) / road.getRoadSpeedSize().toDouble
                        val stdDev: Double = Math.sqrt(temp)

                        if (Math.abs(speed - avgCurrent) <= (2 * stdDev)) {
                            road.incrementCount()
                            road.addRoadSpeed(speed)
                            road.setAverageSpeed(avgCurrent)

                            averageSpeed = avgCurrent
                            count = road.getRoadSpeedSize()
                        }
                    }
                }

                (roadID, averageSpeed, count, timestamp)
            }})

            val endTime = System.nanoTime()
            //             val endMetrics = taskMetrics
            //             val latency = taskContext.taskMetrics.executorRunTime
            val latency = endTime - startTime // Measure the time it took to process the data
            Log.log.warn(s"[Calculator] latency: $latency")

            //            val inputBytes = endMetrics.inputMetrics.bytesRead - startMetrics.inputMetrics.bytesRead
            //            val outputBytes = endMetrics.outputMetrics.bytesWritten - startMetrics.outputMetrics.bytesWritten
            //            val numBytes = outputBytes - inputBytes
            //
            //            val duration = (endTime - startTime) / 1000.0
            //            val bandwidth = numBytes / duration
            //            Log.log.warn(s"[Source] bandwidth: $bandwidth MB/s")

            val elapsedTime = (endTime - startTime) / 1000000000.0
            val mbs: Double = (counter.sum / elapsedTime).toDouble
            val formatted_mbs = String.format("%.5f", mbs)
            Log.log.warn(s"[Calculator] bandwidth: $formatted_mbs MB/s")

            res
        })

    }
}
