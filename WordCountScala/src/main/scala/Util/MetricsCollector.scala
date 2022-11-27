package Util

import org.apache.spark.util.CollectionAccumulator

class MetricsCollector extends java.io.Serializable {
  private var generated: Int = 0
  private var lastTupleTs: Long = 0
  private var epoch: Long = 0
  private var index: Int = 0
  private var bytes: Int = 0
  private var timestamp: Long = 0
  private var rate: Double = 0
  private var formatted_mbs: String = null

  def collectMetrics(data: String, counter: CollectionAccumulator[String]) = {
      counter.add(data)
      timestamp = System.nanoTime
      bytes += counter.value.get(index).getBytes.length
      index += 1
      generated += 1
      lastTupleTs = timestamp
      // set the starting time
      if (generated == 1)
        epoch = System.nanoTime
      data
  }

  def nullifyValues(): Unit = {
    timestamp = 0L
    bytes = 0
    index = 0
    generated = 0
    lastTupleTs = 0L
    epoch = 0L
    formatted_mbs = null
    rate = 0
  }

  def measureThroughput(): Unit = {
    val tElapsed = (lastTupleTs - epoch) / 1e6
    val mbs = (bytes / 1048576).toDouble / (tElapsed / 1000).toDouble
    formatted_mbs = String.format("%.5f", mbs)
    Log.log.warn(s"Measured throughput: ${formatted_mbs} MB/second")
//    this.nullifyValues()
  }

}
