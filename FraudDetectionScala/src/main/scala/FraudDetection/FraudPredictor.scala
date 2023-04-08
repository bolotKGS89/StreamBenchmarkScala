package FraudDetection

import MarkovModelPrediction.{MarkovModelPredictorJava, MarkovModelPredictorScala, ModelBasedPredictor}
import org.apache.spark.streaming.dstream.DStream
import Util.Log
import Util.config.Configuration
import org.apache.hadoop.fs.Path
import org.apache.commons.lang.StringUtils
import org.apache.spark.streaming.StreamingContext

class FraudPredictor extends Serializable {

  def execute(lines: DStream[(String, String, Long)], ssc: StreamingContext, predModel: String, predictorParDeg: Int):
      Unit = {

    val counter = ssc.sparkContext.longAccumulator("Predictor accumulator")

    var predictor: ModelBasedPredictor = null;

    lines.repartition(predictorParDeg).transform({ rdd =>
      if (predModel.equals("mm")) {
        predictor = new MarkovModelPredictorJava()
      }
      val startTime = System.nanoTime()

      val lines = rdd.repartition(predictorParDeg)
          .map { case (entityId, record, timestamp) =>
            val p = predictor.execute(entityId, record)
            (p, entityId, timestamp)
          }.filter { case (prediction, entityId, timestamp) =>
            prediction.isOutlier
          }.map{ case (prediction, entityId, timestamp) =>
            (entityId, prediction.getScore, prediction.getStates.mkString(","), timestamp)
          }

      lines
      }).print(100)


//      val endTime = System.nanoTime()
//      val latency = endTime - startTime // Measure the time it took to process the data
//      Log.log.warn(s"[Predictor] latency: $latency")
//
//      val elapsedTime = (endTime - startTime) / 1000000000.0
//      val mbs: Double = (counter.count / elapsedTime).toDouble
//      val formattedMbs = String.format("%.5f", mbs)
//      Log.log.warn(s"[Predictor] bandwidth: $formattedMbs MB/s")
  }

}
