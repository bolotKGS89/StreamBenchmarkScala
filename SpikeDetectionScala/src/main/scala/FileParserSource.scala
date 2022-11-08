package spikeDetection

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io.FileNotFoundException


class FileParserSource(path: String, ssc: StreamingContext)  {

  def parseDataSet(valueField: String): DStream[(String, String, Long)] = {
    lazy val fieldList: Map[String, Integer] = Map(
      "temp" -> SpikeDetectionConstants.TempField,
      "humid" -> SpikeDetectionConstants.HumidField,
      "light" -> SpikeDetectionConstants.LightField,
      "volt" -> SpikeDetectionConstants.VoltField
    )
    try {
      ssc.textFileStream(path).flatMap(line => {
        val words = line.split("\n").filter(word => word.length >= 8)
        words.map(word => word.split("\\s+")).map((splitWords) => {
          (splitWords(SpikeDetectionConstants.DateField),
            splitWords(SpikeDetectionConstants.TimeField),
            splitWords(SpikeDetectionConstants.EpochField),
            splitWords(SpikeDetectionConstants.DeviceIdField),
            splitWords(SpikeDetectionConstants.TempField),
            splitWords(SpikeDetectionConstants.HumidField),
            splitWords(SpikeDetectionConstants.LightField))
        }).map((dataTuple) => {
          val timestamp = System.nanoTime
          val valueFieldKey = fieldList.get(valueField).get
          val res = valueFieldKey match {
            case valueFieldKey if (valueFieldKey == SpikeDetectionConstants.TempField) => (dataTuple._3, dataTuple._4, timestamp)
            case valueFieldKey if (valueFieldKey == SpikeDetectionConstants.HumidField) => (dataTuple._3, dataTuple._5, timestamp)
            case valueFieldKey if (valueFieldKey == SpikeDetectionConstants.LightField) => (dataTuple._3, dataTuple._6, timestamp)
            //              case _ => (dataTuple._4, dataTuple._8, timestamp)
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
