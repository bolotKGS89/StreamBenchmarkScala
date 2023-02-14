package SpikeDetection

import Constants.SpikeDetectionConstants._
import Util.Log
import Util.MetricsCollector
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer
import java.io.FileNotFoundException

class FileParserSource(path: String, ssc: StreamingContext)  {

  def parseDataSet(valueField: String): DStream[(String, Double, Long)] = {
    lazy val fieldList: Map[String, Integer] = Map(
      "temp" -> DatasetParsing.TempField,
      "humid" -> DatasetParsing.HumidField,
      "light" -> DatasetParsing.LightField,
      "volt" -> DatasetParsing.VoltField
    )
    lazy val valueFieldKey = fieldList.get(valueField).get
    val counter = ssc.sparkContext.longAccumulator

    try {
      ssc.textFileStream(path).transform({ rdd =>
        val startTime = System.nanoTime()

        val words = rdd.flatMap((line) => line.split("\n")).filter((line) => !line.isEmpty)
          .map(word => word.split("\\s+")).filter((splitWords) => splitWords.length == 8)
            .map((splitWords) => {
            Log.log.debug("[Source] tuple: deviceID " + splitWords(DatasetParsing.DeviceIdField) +
              ", property " + valueField + " " + fieldList.get(valueField))
            Log.log.debug("[Source] fields: " +
              splitWords(DatasetParsing.DateField) + " " +
              splitWords(DatasetParsing.TimeField) + " " +
              splitWords(DatasetParsing.EpochField) + " " +
              splitWords(DatasetParsing.DeviceIdField) + " " +
              splitWords(DatasetParsing.TempField) + " " +
              splitWords(DatasetParsing.HumidField) + " " +
              splitWords(DatasetParsing.LightField) + " " +
              splitWords(DatasetParsing.VoltField)
            )

            val timestamp = System.nanoTime
            counter.add(timestamp)

            val res = valueFieldKey match {
              case valueFieldKey if (valueFieldKey == DatasetParsing.TempField) =>
                (splitWords(DatasetParsing.DeviceIdField).toString, splitWords(DatasetParsing.TempField).toDouble, timestamp)
              case valueFieldKey if (valueFieldKey == DatasetParsing.HumidField) =>
                (splitWords(DatasetParsing.DeviceIdField).toString, splitWords(DatasetParsing.HumidField).toDouble, timestamp)
              case valueFieldKey if (valueFieldKey == DatasetParsing.LightField) =>
                (splitWords(DatasetParsing.DeviceIdField).toString, splitWords(DatasetParsing.LightField).toDouble, timestamp)
              case _ =>
                (splitWords(DatasetParsing.DeviceIdField).toString, splitWords(DatasetParsing.VoltField).toDouble, timestamp)
            }

            res
        })
        val endTime = System.nanoTime
        val latency = endTime - startTime // Measure the time it took to process the data
        Log.log.warn(s"[Source] latency: $latency")

        val elapsedTime = (endTime - startTime) / 1000000000.0
        val mbs: Double = (counter.sum / elapsedTime).toDouble
        val formatted_mbs = String.format("%.5f", mbs)
        Log.log.warn(s"[Source] bandwidth: $formatted_mbs MB/s")

        words
      })
    } catch {
      case _: FileNotFoundException | _: NullPointerException => {
        throw new RuntimeException(s"The file $path does not exists")
      }
    }
  }

}

//            (splitWords(DatasetParsing.DateField),
//            splitWords(DatasetParsing.TimeField),
//            splitWords(DatasetParsing.EpochField),
//            splitWords(DatasetParsing.DeviceIdField),
//            splitWords(DatasetParsing.TempField),
//            splitWords(DatasetParsing.HumidField),
//            splitWords(DatasetParsing.LightField),
//            splitWords(DatasetParsing.VoltField))
