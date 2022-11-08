package FraudDetection

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkFraudDetection
{
  def main(args: Array[String]): Unit = {

      if (args.length != 3) {
        System.err.println("Wrong number of arguments: ")
        System.err.println(args.length)
        System.exit(1)
      }

    val inputFile = args(0)
    val outputFile = args(1)
    val regExVal = args(2)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkFraudDetection")
    val ssc = new StreamingContext(sparkConf, Seconds(7))

    val lines = new FileParserSpout(inputFile, ssc).parseDataSet(regExVal)

    val outlierLines = new FraudPredictor().execute(lines)



    ssc.start()
    ssc.awaitTermination()


  }
}
