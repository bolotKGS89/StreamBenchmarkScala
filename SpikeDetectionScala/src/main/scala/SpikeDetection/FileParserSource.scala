package SpikeDetection

import Constants.SpikeDetectionConstants._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io.FileNotFoundException

class FileParserSource(path: String, ssc: StreamingContext)  {

  def parseDataSet(valueField: String): DStream[(String, String, Long)] = {
    lazy val fieldList: Map[String, Integer] = Map(
      "temp" -> DatasetParsing.TempField,
      "humid" -> DatasetParsing.HumidField,
      "light" -> DatasetParsing.LightField,
      "volt" -> DatasetParsing.VoltField
    )

    try {
      ssc.textFileStream(path).transform({ rdd =>
        val words = rdd.flatMap((line) => line.split("\n"))
        words.map(word => word.split("\\s+")).map((splitWords) => {
            (splitWords(DatasetParsing.DateField),
            splitWords(DatasetParsing.TimeField),
            splitWords(DatasetParsing.EpochField),
            splitWords(DatasetParsing.DeviceIdField),
            splitWords(DatasetParsing.TempField),
            splitWords(DatasetParsing.HumidField),
            splitWords(DatasetParsing.LightField),
            splitWords(DatasetParsing.VoltField))
        }).map((dataTuple) => {
          val timestamp = System.nanoTime
          val valueFieldKey = fieldList.get(valueField).get
          val res = valueFieldKey match {
            case valueFieldKey if (valueFieldKey == DatasetParsing.TempField) => (dataTuple._3, dataTuple._4, timestamp)
            case valueFieldKey if (valueFieldKey == DatasetParsing.HumidField) => (dataTuple._3, dataTuple._5, timestamp)
            case valueFieldKey if (valueFieldKey == DatasetParsing.LightField) => (dataTuple._3, dataTuple._6, timestamp)
            case _ => (dataTuple._4, dataTuple._8, timestamp)
          }
          res
        })
      })

    } catch {
      case _: FileNotFoundException | _: NullPointerException => {
        throw new RuntimeException(s"The file $path does not exists")
      }
    }
  }

}
