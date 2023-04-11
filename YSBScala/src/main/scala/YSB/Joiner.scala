package YSB

import Util.Log
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.collection.mutable.HashMap

class Joiner {
    def doJoin(stream: DStream[(String, String, String, String, String, Long, String)], campaignLookup: mutable.HashMap[String, String], parDegree: Int):
    DStream[(String, String, Long)] = {
        var processed = 0L

        stream.transform({ rdd =>
            val startTime = System.nanoTime()

            val res = rdd.repartition(parDegree).filter((campaign) => !campaign._3.equals(null)).flatMap((campaign) => {
                processed += 1
                val adId = campaign._3
                val timestamp = campaign._6
                val campaignId = campaignLookup(adId)
//                if (processed < 20)
//                    System.out.println("campaignId " + campaignId + " adId " + adId + " ts " + timestamp)
                Seq((campaignId, adId, timestamp))
            })

            val endTime = System.nanoTime()
            val latency = endTime - startTime // Measure the time it took to process the data
            Log.log.warn(s"[Joiner] latency: $latency")

            val elapsedTime = (endTime - startTime) / 1000000000.0
            val tuples: Double = (processed / elapsedTime).toDouble
//            val formattedTuples = String.format("%.5f", tuples)
//            Log.log.info(s"[Joiner] bandwidth: $formattedTuples tuples/sec")

            res
        })
    }
}
