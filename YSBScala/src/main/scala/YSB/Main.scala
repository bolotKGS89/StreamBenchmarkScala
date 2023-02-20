package YSB

import Util.Log
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.{Properties, UUID}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SparkYSB {
  def main(args: Array[String]): Unit = {

    var isCorrect = true
    var genRate = -1
    var sampling = 1
    var source_par_deg = 1
    var filter_par_deg = 1
    var joiner_par_deg = 1
    var agg_par_deg = 1
    var sink_par_deg = 1

    if (args.length == 10) {
      if (!(args(0) == "--rate")) isCorrect = false
      else try genRate = args(1).toInt
      catch {
        case e: NumberFormatException =>
          isCorrect = false
      }
      if (!(args(2) == "--sampling")) isCorrect = false
      else try sampling = args(3).toInt
      catch {
        case e: NumberFormatException =>
          isCorrect = false
      }
      if (!(args(4) == "--parallelism")) isCorrect = false
      else try {
        source_par_deg = args(5).toInt
        filter_par_deg = args(6).toInt
        joiner_par_deg = args(7).toInt
        agg_par_deg = args(8).toInt
        sink_par_deg = args(9).toInt
      } catch {
        case e: NumberFormatException =>
          isCorrect = false
      }
    }
    else {
      Log.log.error("Error in parsing the input arguments")
      System.exit(1)
    }

    if (!isCorrect) {
      Log.log.error("Error in parsing the input arguments")
      System.exit(1)
    }

    val props = new Properties()
    val resourceStream = getClass.getResourceAsStream("/YSB.properties")
    props.load(resourceStream)

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("YSBScala")

    val runTimeSec: Long = props.getProperty("yb.runtime_sec").toLong
    // initialize the HashMap of campaigns// initialize the HashMap of campaigns
    val numCampaigns: Int = props.getProperty("yb.numKeys").toInt

    val campaignAdSeq: ListBuffer[CampaignAd] = generateCampaignMapping(numCampaigns)
    val campaignLookup: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
    for (i <- 0 until campaignAdSeq.size) {
      campaignLookup.put(campaignAdSeq(i).ad_id, campaignAdSeq(i).campaign_id)
    }

    val initialTime = System.nanoTime

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val filterStream = new YSBSource().generateDataSet(ssc, source_par_deg, campaignAdSeq, runTimeSec, initialTime)

    val joinedStream = new Filter().doFilter(filterStream, filter_par_deg)

    val winAggregateStream = new Joiner().doJoin(joinedStream, campaignLookup, joiner_par_deg)

    val consoleSinkStream = new WinAggregate().doWinAggregate(winAggregateStream, agg_par_deg, initialTime)

    val endStream = new ConsoleSink().doConsole(consoleSinkStream, sink_par_deg, sampling)

    endStream.print(100)

    ssc.start()
    ssc.awaitTermination()
  }

  private def generateCampaignMapping(numCampaigns: Int): ListBuffer[CampaignAd] = {
    val campaignArray: Array[CampaignAd] = new Array[CampaignAd](numCampaigns * 10)
    for (i <- 0 until numCampaigns) {
      val campaign: String = UUID.randomUUID.toString
      for (j <- 0 until 10) {
        campaignArray((10 * i) + j) = new CampaignAd(UUID.randomUUID.toString, campaign)
      }
    }

    val listBuffer = ListBuffer[CampaignAd]()
    listBuffer ++= campaignArray
  }

}
