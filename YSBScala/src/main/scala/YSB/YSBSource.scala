package YSB

import Util.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io.FileNotFoundException
import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class YSBSource() extends Serializable{

  def generateDataSet(ssc: StreamingContext, parDegree: Int, campaigns: ListBuffer[CampaignAd], runTimeSec: Long, initialTime: Long):
      DStream[(String, String, String, String, String, Long, String)] = {
    val AD_TYPES: List[String] = List("banner", "modal", "sponsored-search", "mail", "mobile")
    val EVENT_TYPES: List[String] = List("view", "click", "purchase")
    val adTypeLength = AD_TYPES.size
    val eventTypeLength = EVENT_TYPES.size
    val campaignLength = campaigns.size
    val uuid = UUID.randomUUID.toString;
    var i = 0
    var j = 0
    var k = 0
    var initTime = initialTime
    var ts: Long = 0
    var generated = 0L
    var lastTime = 0L

    try {
        val rddList = campaigns.toList
        val rddQueue = new mutable.Queue[RDD[CampaignAd]]
        val rdd = ssc.sparkContext.parallelize(rddList)

        rddQueue += rdd
        val stream = ssc.queueStream(rddQueue).transform({ rdd =>
          val startTime = System.nanoTime()
          val res = rdd.repartition(parDegree).map((campaign) => {
            i += 1
            j += 1
            k += 1
            if (i >= campaignLength) i = 0
            if (j >= adTypeLength) j = 0
            if (k >= eventTypeLength) k = 0
            ts = initTime + (System.nanoTime - initTime)
            val ad_id = campaign.ad_id // ad id for the current event index
            val ad_type = AD_TYPES(j) // current adtype for event index
            val event_type = EVENT_TYPES(k) // current event type for event index
            val ip = "255.255.255.255"
            generated += 1
            lastTime = System.nanoTime
            (uuid, uuid, ad_id, ad_type, event_type, ts, ip)
          })

          val endTime = System.nanoTime()
          val latency = endTime - startTime // Measure the time it took to process the data
          Log.log.info(s"[Source] latency: $latency")

          val elapsedTime = (endTime - startTime) / 1000000000.0
          val tuples: Double = (generated / elapsedTime).toDouble
          val formattedTuples = String.format("%.5f", tuples)
          Log.log.info(s"[Source] bandwidth: $formattedTuples tuples/sec")

          res
        })
      stream

    } catch {
      case _: FileNotFoundException | _: NullPointerException => {
        throw new RuntimeException(s"Exception")
      }
    }
  }
}
