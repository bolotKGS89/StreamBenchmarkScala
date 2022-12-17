package FraudDetection

import Util.Log
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io
import java.io.FileNotFoundException
import scala.collection.mutable.ListBuffer

class FileParserSpout(path: String, ssc: StreamingContext) {

  def parseDataSet(splitRegex: String): DStream[(String, String, Long)] = {
    lazy val records = new ListBuffer[String]
    lazy val entities = new ListBuffer[String]
    var index = 0
    var generated = 0
    var lastTupleTs: Long = 0L
    var rate = 1
    var ntExecution = 0
    var epoch: Long = 0
    try {
      ssc.textFileStream(path).transform({ rdd =>
        rdd.map(line => {
          val splitLines = line.split(splitRegex, 2)
          entities.addOne(splitLines(0))
          records.addOne(splitLines(1))
          splitLines })
          .map((data) => {
          val timestamp = System.nanoTime
          val finalTup = (entities(index), records(index), timestamp)
          index += 1
          generated += 1
          lastTupleTs = timestamp
          if (rate != 0) { // not full speed
            val delay_nsec = ((1.0d / rate) * 1e9).toLong
//            activeDelay(delay_nsec)
            val t_start = System.nanoTime
            var t_now = 0L
            var end = false
            while (!end) {
              t_now = System.nanoTime
              end = (t_now - t_start) >= delay_nsec
            }
          }
          // check the dataset boundaries// check the dataset boundaries
          if (index >= entities.size) {
            index = 0
            ntExecution += 1
          }
          // set the starting time// set the starting time
          if (generated == 1) {
            epoch = System.nanoTime
          }

          finalTup
        })
      })


    } catch {
      case _: FileNotFoundException | _: NullPointerException => {
        Log.log.error(s"The file {} does not exists $path")
        throw new RuntimeException(s"The file $path does not exists")
      }
    }

  }
   private def activeDelay(nsecs: Double): Unit  = {
       val t_start = System.nanoTime
       var t_now = 0L
       var end = false
       while (!end) {
         t_now = System.nanoTime
         end = (t_now - t_start) >= nsecs
       }
  }
}
