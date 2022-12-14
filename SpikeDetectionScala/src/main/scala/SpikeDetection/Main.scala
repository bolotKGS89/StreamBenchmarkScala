package SpikeDetection

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkSpikeDetection {
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
        System.err.println("Wrong number of arguments: ")
        System.err.println(args.length)
        System.exit(1)
      }

    val inputFile = args(0)
    val outputFile = args(1)
    val valueField = args(2)
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSpikeDetection")
    val ssc = new StreamingContext(sparkConf, Seconds(4))

    //1st stage
    val tuples = new FileParserSource(inputFile, ssc).parseDataSet(valueField)

    //2nd stage
    val avgTuples = new MovingAverage().execute(tuples)

    //3rd stage
    val filteredTuples = new SpikeDetection().execute(avgTuples)

    //4th stage
    // sampling should be cmd argument
    val output = new ConsoleSink(100L).print(filteredTuples)

    output.print(100)

    ssc.start()
    ssc.awaitTermination()

  }
}
