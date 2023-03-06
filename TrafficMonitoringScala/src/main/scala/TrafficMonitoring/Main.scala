package TrafficMonitoring

import Constants.TrafficMonitoringConstants.{City, Conf}
import Util.Log
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {
      var isCorrect = true
      var genRate = -1
      var sampling = 1
      var sourceParDeg = 1
      var splitterParDeg = 1
      var counterParDeg = 1
      var sinkParDeg = 1

      if (args.length == 9) {
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
          sourceParDeg = args(5).toInt
          splitterParDeg = args(6).toInt
          counterParDeg = args(7).toInt
          sinkParDeg = args(8).toInt
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
    val resourceStream = getClass.getResourceAsStream("/tm.properties")
    props.load(resourceStream)

    val inputDirectory = props.getProperty("tm.spout.beijing")
    val city = props.getProperty(City.BEIJING)

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkTrafficMonitoring")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = new FileParser(inputDirectory, ssc, sourceParDeg, city).parseDataSet()

    val mapMatchLines = new MapMatching(lines, sourceParDeg, city).execute()

    mapMatchLines.print(5)

    ssc.start()
    ssc.awaitTermination()

    }
}