package FraudDetection

import Util.Log
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

object SparkFraudDetection
{
  def main(args: Array[String]): Unit = {

    var isCorrect = true
    var genRate = -1
    var sampling = 1
    var sourceParDeg = 1
    var predictorParDeg = 1
    var sinkParDeg = 1

    if (args.length == 8) {
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
        predictorParDeg = args(6).toInt
        sinkParDeg = args(7).toInt
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
    val resourceStream = getClass.getResourceAsStream("/frauddetection/fd.properties")
    props.load(resourceStream)

    val inputFile = props.getProperty("fd.spout.path")
    val predModel = props.getProperty("fd.predictor.model")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkFraudDetection")
    val ssc = new StreamingContext(sparkConf, Seconds(15))

    //1st stage
    val lines = new FileParserSpout(inputFile, ssc).parseDataSet(",", sourceParDeg)

    //2nd stage
    val outlierLines = new FraudPredictor().execute(lines, ssc, predModel, predictorParDeg)

    //3rd stage
    // sampling should be cmd argument
    val output = new ConsoleSink(sampling).print(outlierLines, sinkParDeg)
    output.print(100)

    ssc.start()
    ssc.awaitTermination()
  }
}
