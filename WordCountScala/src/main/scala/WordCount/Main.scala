package WordCount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Wrong number of arguments: ")
      System.err.println(args.length)
      System.exit(1)
    }

    val inputFile = args(0)
    val outputFile = args(1)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //1st stage
    val lines = new FileParserSource(inputFile, ssc).parseDataSet()
    //2nd stage
    val words = new Splitter(lines, ssc).execute()
    //3rd stage
    val wordCounts = new Counter(words, ssc, 100L).count()
    //4th stage

    wordCounts.print(10)

    ssc.start()
    ssc.awaitTermination()

  }

}