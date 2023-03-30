package TrafficMonitoring

import Util.{Log, Sampler}
import org.apache.spark.streaming.dstream.DStream

class ConsoleSink(lines: DStream[(Int, Int, Int, Long)], genRate: Int, parallelismDegree: Int, samplingRate: Int) extends Serializable {
    def execute(): DStream[(Int, Int, Int, Long)] = {

        lines.transform({ rdd =>
            val startTime = System.nanoTime()
            val counter = rdd.sparkContext.longAccumulator
            val sampler = new Sampler(samplingRate)
            var processed = 0

            val res = rdd.repartition(parallelismDegree).map({ case (roadID, speed, count, timestamp) => {
                val now = System.nanoTime

                sampler.add((now - timestamp).toDouble / 1e3, now)
                processed += 1
                (roadID, speed, count, timestamp)
            }})

            val endTime = System.nanoTime()
            //             val endMetrics = taskMetrics
            //             val latency = taskContext.taskMetrics.executorRunTime
            val latency = endTime - startTime // Measure the time it took to process the data
            Log.log.warn(s"[Console] latency: $latency")

            //            val inputBytes = endMetrics.inputMetrics.bytesRead - startMetrics.inputMetrics.bytesRead
            //            val outputBytes = endMetrics.outputMetrics.bytesWritten - startMetrics.outputMetrics.bytesWritten
            //            val numBytes = outputBytes - inputBytes
            //
            //            val duration = (endTime - startTime) / 1000.0
            //            val bandwidth = numBytes / duration
            //            Log.log.warn(s"[Source] bandwidth: $bandwidth MB/s")

            val elapsedTime = (endTime - startTime) / 1000000000.0
            val mbs: Double = (counter.sum / elapsedTime).toDouble
//            val formatted_mbs = String.format("%.5f", mbs)
//            Log.log.warn(s"[Console] bandwidth: $formatted_mbs MB/s")

            res
        })
    }
}
