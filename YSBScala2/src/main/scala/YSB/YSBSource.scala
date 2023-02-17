package YSB

import Util.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io.FileNotFoundException
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class YSBSource(ssc: StreamingContext, parDegree: Int, campaigns: ListBuffer[CampaignAd], runTimeSec: Long, initialTime: Long) {

  private val AD_TYPES: util.List[String] = util.Arrays.asList("banner", "modal", "sponsored-search", "mail", "mobile")
  private val EVENT_TYPES: util.List[String] = util.Arrays.asList("view", "click", "purchase")

  def generateDataSet(): DStream[CampaignAd] = {
    try {
        val rddList = campaigns.toList
        val rddQueue = new mutable.Queue[RDD[CampaignAd]]
        val rdd = ssc.sparkContext.parallelize(rddList)

        rddQueue += rdd
        val stream = ssc.queueStream(rddQueue).transform({ rdd =>
          val startTime = System.nanoTime()

          val endTime = System.nanoTime()
          val latency = endTime - startTime // Measure the time it took to process the data
              Log.log.warn(s"[Source] latency: $latency")
          rdd
        })
      stream

    } catch {
      case _: FileNotFoundException | _: NullPointerException => {
        throw new RuntimeException(s"Exception")
      }
    }
  }
}
