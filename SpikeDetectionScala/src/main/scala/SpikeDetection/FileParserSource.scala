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

    val date = new ListBuffer[String]
    val time = new ListBuffer[String]
    val epoch = new ListBuffer[Int]
    val devices = new ListBuffer[String]
    val temperature = new ListBuffer[Double]
    val humidity = new ListBuffer[Double]
    val light = new ListBuffer[Double]
    val voltage = new ListBuffer[Double]
    val metricsCollector = new MetricsCollector()
    var index = 0

    try {
      ssc.textFileStream(path).transform({ rdd =>
        val words = rdd.flatMap((line) => line.split("\n")).filter((line) => !line.isEmpty)
          words.map(word => word.split("\\s+")).filter((splitWords) => splitWords.length == 8)
            .map((splitWords) => {
            date.addOne(splitWords(DatasetParsing.DateField))
            time.addOne(splitWords(DatasetParsing.TimeField))
            epoch.addOne(splitWords(DatasetParsing.EpochField).toInt)
            devices.addOne(splitWords(DatasetParsing.DeviceIdField).toString)
            temperature.addOne(splitWords(DatasetParsing.TempField).toDouble)
            humidity.addOne(splitWords(DatasetParsing.HumidField).toDouble)
            light.addOne(splitWords(DatasetParsing.LightField).toDouble)
            voltage.addOne(splitWords(DatasetParsing.VoltField).toDouble)
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
            null
        }).map((data) => {
            val timestamp = System.nanoTime
            val res = valueFieldKey match {
              case valueFieldKey if (valueFieldKey == DatasetParsing.TempField) => (devices(index), temperature(index), timestamp)
              case valueFieldKey if (valueFieldKey == DatasetParsing.HumidField) => (devices(index), humidity(index), timestamp)
              case valueFieldKey if (valueFieldKey == DatasetParsing.LightField) => (devices(index), light(index), timestamp)
              case _ => (devices(index), voltage(index), timestamp)
            }
            index += 1
            // rate is cmd argument
            metricsCollector.collectMetrics(timestamp, 0, devices)
            res
        })
        //metricsCollector.measureThroughput()
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
