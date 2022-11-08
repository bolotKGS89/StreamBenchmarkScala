package org.example;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * WordCount
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("WordCount")
                .config("spark.master", "local")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<String> lines = spark.read().option("linesep", "\r")
                .text("D:\\Thesis\\wordCount\\gutenberg-1G.txt").as(Encoders.STRING());

        lines.show();

        spark.close();
    }
}
