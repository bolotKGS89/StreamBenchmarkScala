package wordCount

import Util.Log
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io.FileNotFoundException

class FileParserSource(path: String, ssc: StreamingContext)  {

  def parseDataSet(): DStream[(String)] = {
    try {
      var generated: Int = 0
      var lastTupleTs: Long = 0
      var epoch: Long = 0
      var index: Int = 0
      var bytes: Int = 0
      var lineLst: List[String] = List()
      var timestamp: Long = 0
      var rate: Double = 0
      var formatted_mbs: String = null

      ssc.textFileStream(path).transform({ rdd =>

        val lines = rdd.flatMap((line) => line.split("\n")).filter((line) => !line.isEmpty)
        .map((line) => {
          lineLst  = line :: lineLst
          line
        })
        .map((line) => {
          timestamp = System.nanoTime
          bytes += lineLst(index).getBytes().length
          index += 1
          generated += 1
          lastTupleTs = timestamp
          // set the starting time
          if (generated == 1)
            epoch = System.nanoTime

          if (index >= lineLst.length) {
            index = 0
          }

          line
        })

        rate = generated / ((lastTupleTs - epoch) / 1e9)
        val tElapsed = (lastTupleTs - epoch) / 1e6
        val mbs = (bytes / 1048576).toDouble / (tElapsed / 1000).toDouble
        formatted_mbs = String.format("%.5f", mbs)
        Log.log.warn(s"Measured throughput: ${rate.round} lines/second ${formatted_mbs} MB/second")

        lines
      })

    } catch {
      case _: FileNotFoundException | _: NullPointerException => {
        Log.log.error(s"The file {} does not exists $path")
        throw new RuntimeException(s"The file $path does not exists")
      }
    }
  }
}
