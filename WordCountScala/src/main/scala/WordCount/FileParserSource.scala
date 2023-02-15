package WordCount

import Util.Log
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io.FileNotFoundException

class FileParserSource(path: String, ssc: StreamingContext, parDegree: Int) {

  def parseDataSet(): DStream[(String, Long)] = {
    var timestamp = 0L
    try {
          val counter = ssc.sparkContext.longAccumulator

          ssc.textFileStream(path).transform({ rdd =>
             val startTime = System.nanoTime()

             val res = rdd.repartition(parDegree).flatMap((line) => {
               val splitLine = line.split("\n").filter((line) => !line.isEmpty)
               timestamp = System.nanoTime
               counter.add(line.getBytes.length)
               splitLine })
              .map((line) => (line, timestamp))

             val endTime = System.nanoTime()
             val latency = endTime - startTime // Measure the time it took to process the data
             Log.log.warn(s"[Source] latency: $latency")

             val elapsedTime = (endTime - startTime) / 1000000000.0
             val mbs: Double = (counter.sum / elapsedTime).toDouble
             val formatted_mbs = String.format("%.5f", mbs)
             Log.log.warn(s"[Source] bandwidth: $formatted_mbs MB/s")

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
