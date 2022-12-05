package Util

import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable.ListBuffer

class MetricsCollector extends java.io.Serializable {
    private var generated: Int = 0
    private var lastTupleTs: Long = 0
    private var epoch: Long = 0
    private var index: Int = 0
    private var bytes: Int = 0
    private var timestamp: Long = 0
    private var rate: Double = 0
    private var ntExecution: Int = 0

    def collectMetrics(timestamp: Long, rate: Double, devices: ListBuffer[String]): Unit = {
      index += 1
      generated += 1
      lastTupleTs = timestamp
      if (rate != 0) { // not full speed
        val delayNsec = ((1.0d / rate) * 1e9).toLong
        activeDelay(delayNsec)
      }
      if (index >= devices.size) {
        index = 0
        ntExecution += 1
      }

      // set the starting time
      if (generated == 1)
        epoch = System.nanoTime
    }

    private def activeDelay(nSecs: Double): Unit = {
      val tStart = System.nanoTime
      var tNow = 0L
      var end = false
      while (!end) {
        tNow = System.nanoTime
        end = (tNow - tStart) >= nSecs
      }
    }

    def nullifyValues(): Unit = {
      timestamp = 0L
      bytes = 0
      index = 0
      generated = 0
      lastTupleTs = 0L
      epoch = 0L
      rate = 0
    }

    def measureThroughput(): Unit = {
      val rate: Double = generated / ((lastTupleTs - epoch) / 1e9) // per second
      val tElapsed = ((lastTupleTs - epoch) / 1e6).toLong;
      Log.log.info("[Source] execution time: " + tElapsed +
                " ms, generations: " + ntExecution +
                ", generated: " + generated +
                ", bandwidth: " + generated / (tElapsed / 1000) +  // tuples per second
                " tuples/s");

      Log.log.info("Measured throughput: " + rate.toInt + " tuples/second")
    }

}
