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
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //1st stage
    val lines = new FileParserSpout(inputFile, ssc).parseDataSet(regExVal)

    //2nd stage
    val outlierLines = new FraudPredictor().execute(lines)

    //3rd stage
    // sampling should be cmd argument
    new ConsoleSink(100L).print(outlierLines)

    ssc.start()
    ssc.awaitTermination()
  }
}
