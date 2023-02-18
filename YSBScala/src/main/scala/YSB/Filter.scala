package YSB

import Util.Log
import org.apache.spark.streaming.dstream.DStream

class Filter extends Serializable{

  def doFilter(stream: DStream[(String, String, String, String, String, Long, String)], parDegree: Int)
      : DStream[(String, String, String, String, String, Long, String)] = {
      var processed = 0L

      stream.transform({ rdd =>
        val startTime = System.nanoTime()

        val res = rdd.repartition(parDegree).filter((campaign) => {
          processed += 1
          campaign._5.equals("view")
        })

        processed += 1
        val endTime = System.nanoTime()
        val latency = endTime - startTime // Measure the time it took to process the data
        Log.log.info(s"[Filter] latency: $latency")

        val elapsedTime = (endTime - startTime) / 1000000000.0
        val tuples: Double = (processed / elapsedTime).toDouble
        val formattedTuples = String.format("%.5f", tuples)
        Log.log.info(s"[Filter] bandwidth: $formattedTuples tuples/sec")

        res
      })
  }
}
