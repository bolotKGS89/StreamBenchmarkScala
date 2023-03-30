package YSB

import Util.{Log, MetricGroup, Sampler}
import org.apache.spark.streaming.dstream.DStream

class ConsoleSink {
  def doConsole(stream: DStream[(String, Long, Long)], parDegree: Int, sampling: Int): DStream[(String, Long, Long)] = {
      var processed = 0L
      val sampler = new Sampler(sampling);
      var t_end = 0L
      var estimated_bw = 0L
      stream.transform({ rdd =>
        val startTime = System.nanoTime()

        val res = rdd.repartition(parDegree).map((data) => {
            val counter = data._2
            val timestamp = data._3

            val now = System.nanoTime
            sampler.add((now - timestamp).toDouble / 1e3, now)
            processed += 1
            estimated_bw += counter * 3
            t_end = System.nanoTime
            data
        })

        MetricGroup.add("latency", sampler)

        val endTime = System.nanoTime()
        val latency = endTime - startTime // Measure the time it took to process the data
        Log.log.warn(s"[ConsoleSink] latency: $latency")

        val elapsedTime = (endTime - startTime) / 1000000000.0
        val tuples: Double = (processed / elapsedTime).toDouble
//        val formattedTuples = String.format("%.5f", tuples)
//        Log.log.info(s"[ConsoleSink] bandwidth: $formattedTuples tuples/sec")

        res
      })
  }

}
