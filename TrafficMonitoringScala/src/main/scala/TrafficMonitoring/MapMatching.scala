package TrafficMonitoring

import Constants.TrafficMonitoringConstants
import Constants.TrafficMonitoringConstants.Conf
import RoadModel.{GPSRecord, RoadGridList}
import Util.Log
import org.apache.spark.streaming.dstream.DStream
import Util.config.Configuration

import java.io.IOException
import java.sql.SQLException
import java.util
import java.util.HashMap

class MapMatching(lines: DStream[(String, Double, Double, Double, Int, Long)], parDegree: Int, city: String) extends Serializable {

    def execute(): DStream[(Int, Int, Long)] = {
        val config = new Configuration
        val roads = new util.HashMap[Integer, Integer]

        val cityShapefile = TrafficMonitoringConstants.BEIJING_SHAPEFILE
        val minLat = config.getDouble(Conf.MAP_MATCHER_BEIJING_MIN_LAT, 39.689602)
        val maxLat = config.getDouble(Conf.MAP_MATCHER_BEIJING_MAX_LAT, 40.122410)
        val minLon = config.getDouble(Conf.MAP_MATCHER_BEIJING_MIN_LON, 116.105789)
        val maxLon = config.getDouble(Conf.MAP_MATCHER_BEIJING_MAX_LON, 116.670021)
        var sectors: RoadGridList = null

        var dif_keys = 0
        var all_keys = 0
        var processed = 0

        try {
            sectors = new RoadGridList(config, cityShapefile)
        } catch {
            case _: SQLException | _: IOException => {
                throw new RuntimeException(s"Error while loading shape file")
            }
        }

        Log.log.
          debug(s"[MapMatch] Sectors:  $sectors Bounds ( $city case): [$minLat, $maxLat] [$minLon, $maxLon])")

        lines.transform({ rdd =>
            val startTime = System.nanoTime()
            val counter = rdd.sparkContext.longAccumulator

            val res = rdd.repartition(parDegree)
              .filter({ case (vehicleID, latitude, longitude, speed, bearing, timestamp) => {
                Log.log.debug(s"[MapMatch] tuple: vehicleID $vehicleID, lat $latitude, lon $longitude, " +
                  s"speed $speed, dir $bearing, ts $timestamp")
                processed += 1
                (speed > 0) && (longitude < maxLon || longitude > minLon || latitude < maxLat || latitude > minLat)
              }}).filter({ case (vehicleID, latitude, longitude, speed, bearing, timestamp) => {
                val record = new GPSRecord(longitude, latitude, speed, bearing)
                val roadID = sectors.fetchRoadID(record)
//                val roadID = 3
                if (roadID != 1) true else false
              }}).map({ case (_, latitude, longitude, speed, bearing, timestamp) => {
                try {
                    val record = new GPSRecord(longitude, latitude, speed, bearing)
                    val roadID = sectors.fetchRoadID(record)
//                    val roadID = 1
                    if (roads.containsKey(roadID)) {
                        val count = roads.get(roadID)
                        roads.put(roadID, count + 1)
                    }
                    else {
                        roads.put(roadID, 1)
                        dif_keys += 1
                    }
                    all_keys += 1
                    (roadID, speed.toInt, timestamp)
                } catch {
                    case e: SQLException => {
                        throw new RuntimeException("Unable to fetch road ID", e)
                    }
                }
            }})

            val endTime = System.nanoTime()
            //             val endMetrics = taskMetrics
            //             val latency = taskContext.taskMetrics.executorRunTime
            val latency = endTime - startTime // Measure the time it took to process the data
            Log.log.warn(s"[MapMatch] latency: $latency")

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
            Log.log.warn(s"[MapMatch] bandwidth: $formatted_mbs MB/s")
            res
        })
    }
}
