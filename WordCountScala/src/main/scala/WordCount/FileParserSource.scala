package WordCount

import Util.Log
import Util.MetricsCollector

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io.FileNotFoundException

class FileParserSource(path: String, ssc: StreamingContext) {

  def parseDataSet(): DStream[(String, Long)] = {
    var timestamp = 0L
    val metrics = new MetricsCollector
    try {
        ssc.textFileStream(path).transform({ rdd =>
          val counter = ssc.sparkContext.collectionAccumulator[String]
          rdd.flatMap((line) => {
           val splitLine = line.split("\n").filter((line) => !line.isEmpty)
           timestamp = System.nanoTime
           splitLine })
        .map((line) => {
          metrics.collectMetrics(line, counter)
          (line, timestamp)
        })
      })
//      MetricsCollector.measureThroughput()
    } catch {
      case _: FileNotFoundException | _: NullPointerException => {
        Log.log.error(s"The file {} does not exists $path")
        throw new RuntimeException(s"The file $path does not exists")
      }
    }
  }
}
