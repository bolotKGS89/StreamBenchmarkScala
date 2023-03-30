package TrafficMonitoring

import Constants.TrafficMonitoringConstants.{BeijingParsing, City}
import Util.Log
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io.FileNotFoundException
import scala.collection.mutable.Queue

class FileParser(path: String, ssc: StreamingContext, parDegree: Int, city: String) extends Serializable{
//
  def parseDataSet(): DStream[(String, Double, Double, Double, Int, Long)] = {
    var timestamp = 0L
    try {
      val counter = ssc.sparkContext.longAccumulator
      val rdd = ssc.sparkContext.textFile(path)

      ssc.queueStream(
        Queue(rdd)
      ).transform({ rdd =>
        //            val taskContext = TaskContext.get
        val startTime = System.nanoTime()
        //             val startMetrics = taskMetrics city == City.BEIJING &&

        val res = rdd.repartition(parDegree).map((line) => {
          val splitLine = line.split(",")
          timestamp = System.nanoTime
          counter.add(line.getBytes.length)
          splitLine
        }).filter((splitLine) => splitLine.length >= 7).map((line) => {

          Log.log.debug("[Source] Beijing Fields: {} ; {} ; {} ; {} ; {}",
            line(BeijingParsing.B_VEHICLE_ID_FIELD),
            line(BeijingParsing.B_LATITUDE_FIELD),
            line(BeijingParsing.B_LONGITUDE_FIELD),
            line(BeijingParsing.B_SPEED_FIELD),
            line(BeijingParsing.B_DIRECTION_FIELD))

          val timestamp = System.nanoTime

          (line(BeijingParsing.B_VEHICLE_ID_FIELD),
            line(BeijingParsing.B_LATITUDE_FIELD).toDouble,
            line(BeijingParsing.B_LONGITUDE_FIELD).toDouble,
            line(BeijingParsing.B_SPEED_FIELD).toDouble,
            line(BeijingParsing.B_DIRECTION_FIELD).toInt,
            timestamp)
        })

        val endTime = System.nanoTime()
        //             val endMetrics = taskMetrics
        //             val latency = taskContext.taskMetrics.executorRunTime
        val latency = endTime - startTime // Measure the time it took to process the data
        Log.log.warn(s"[Source] latency: $latency")

        //            val inputBytes = endMetrics.inputMetrics.bytesRead - startMetrics.inputMetrics.bytesRead
        //            val outputBytes = endMetrics.outputMetrics.bytesWritten - startMetrics.outputMetrics.bytesWritten
        //            val numBytes = outputBytes - inputBytes
        //
        //            val duration = (endTime - startTime) / 1000.0
        //            val bandwidth = numBytes / duration
        //            Log.log.warn(s"[Source] bandwidth: $bandwidth MB/s")

        val elapsedTime = (endTime - startTime) / 1000000000.0
        val mbs: Double = (counter.sum / elapsedTime).toDouble
//        val formatted_mbs = String.format("%.5f", mbs)
//        Log.log.warn(s"[Source] bandwidth: $formatted_mbs MB/s")

        res
      })

    } catch {
      case _: FileNotFoundException | _: NullPointerException => {
        Log.log.error(s"The file {} does not exists $path")
        throw new RuntimeException(s"The file $path does not exists")
      }
    }
  }

}
