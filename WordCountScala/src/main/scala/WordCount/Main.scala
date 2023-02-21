package WordCount

import org.apache.spark.streaming.{Seconds, StreamingContext}
import Util.Log
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties


object SparkWordCount {
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
    val resourceStream = getClass.getResourceAsStream("/wc.properties")
    props.load(resourceStream)

    val inputDirectory = props.getProperty("wc.source.path")

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //1st stage
    val lines = new FileParserSource(inputDirectory, ssc, sourceParDeg).parseDataSet()
    //2nd stage
    val words = new Splitter(lines, ssc, splitterParDeg).execute()
    //3rd stage
    val wordCounts = new Counter(words, ssc, sampling, counterParDeg).count()
    //4th stage
    // Should I use

    // Should I use MetricGroup and write them in .json

    wordCounts.foreachRDD(rdd => {
      // Process each RDD in the DStream
      rdd.foreach(line => {
        // Process each line in the RDD
        println(line)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

}