package wordCount

import org.apache.spark.streaming.dstream.DStream

class Counter(words: DStream[String]) {
  def count(): DStream[(String, Int)] = {
    words.transform { rdd =>
      val tStart = System.nanoTime
      var bytes = 0
      var words = 0
      val pairs = rdd.map(word => {
        bytes += word.getBytes().length
        words += 1
        (word, 1)
      }).reduceByKey(_ + _)
      val tEnd = System.nanoTime

      val t_elapsed = (tStart - tEnd) / 1000000 // elapsed time in milliseconds

      val mbs = (bytes / 1048576).toDouble / (t_elapsed / 1000).toDouble
      val formatted_mbs = String.format("%.5f", mbs)
      pairs
    }
  }
}
