package spikeDetection

import org.apache.spark.streaming.dstream.DStream

class ConsoleSink {
  def print(filteredTuples: DStream[(String, Double, Double, Long)]): Unit = {
    filteredTuples.print(50)
  }
}
