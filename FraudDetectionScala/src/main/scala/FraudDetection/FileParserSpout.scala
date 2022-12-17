package FraudDetection

import Util.Log
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io.FileNotFoundException

class FileParserSpout(path: String, ssc: StreamingContext) {
  def parseDataSet(splitRegex: String): DStream[(String, String, Long)] = {
    try {
      ssc.textFileStream(path).map(line => line.split(splitRegex, 2)).map((splitLines) => (splitLines(0), splitLines(1), System.nanoTime))
    } catch {
      case _: FileNotFoundException | _: NullPointerException => {
        Log.log.error(s"The file {} does not exists $path")
        throw new RuntimeException(s"The file $path does not exists")
      }
    }

  }
}
