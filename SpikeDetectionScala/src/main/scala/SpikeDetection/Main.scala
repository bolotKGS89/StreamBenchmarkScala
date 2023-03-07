package SpikeDetection

import Util.Log
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

object SparkSpikeDetection {
  def main(args: Array[String]): Unit = {

    var isCorrect = true
    var genRate = -1
    var sampling = 1
    var sourceParDeg = 1
    var mvgAvgParDeg = 1
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
        mvgAvgParDeg = args(6).toInt
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
    val resourceStream = getClass.getResourceAsStream("/sd.properties")
    props.load(resourceStream)

    val inputFile = props.getProperty("sd.spout.path")
    val valueField = props.getProperty("sd.parser.value_field")
    val spikeThreshold = props.getProperty("sd.spike_detector.threshold").toDouble

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSpikeDetection")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //1st stage
    val tuples = new FileParserSource().parseDataSet(inputFile, ssc, sourceParDeg, valueField)

    //2nd stage
    val avgTuples = new MovingAverage().execute(tuples, mvgAvgParDeg)

    //3rd stage
    val filteredTuples = new SpikeDetection().execute(avgTuples, counterParDeg, spikeThreshold)

    //4th stage
    // sampling should be cmd argument
    val output = new ConsoleSink(sampling).print(filteredTuples, sinkParDeg)

    output.print(100)

    ssc.start()
    ssc.awaitTermination()

  }
}
