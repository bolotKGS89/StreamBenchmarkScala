package FraudDetection

import MarkovModelPrediction.{MarkovModelPredictor, ModelBasedPredictor}
import org.apache.spark.streaming.dstream.DStream
import Util.Log
import org.apache.commons.lang.StringUtils
import org.apache.spark.streaming.StreamingContext

class FraudPredictor extends Serializable {

  private var predictor: ModelBasedPredictor = null;

  def execute(lines: DStream[(String, String, Long)], ssc: StreamingContext, predModel: String, predictorParDeg: Int): DStream[(String, Double, String, Long)] = {

    val counter = ssc.sparkContext.longAccumulator("Predictor accumulator")

    lines.transform({ rdd =>
      val startTime = System.nanoTime()
      val lines = rdd.repartition(predictorParDeg).map({ case(entityId, record, timestamp) => {

        if (predModel.equals("mm")) {
          predictor = new MarkovModelPredictor()
        }

        val p = predictor.execute(entityId, record)
        Log.log.debug(s"[Predictor] tuple: entityID $entityId record $record ts $timestamp")

        (p, entityId, timestamp)
      }}).filter({ case(prediction, entityId, timestamp) => {
        prediction.isOutlier
      }}).map({ case(prediction, entityId, timestamp) => {
        Log.log.debug(s"[Predictor] outlier: entityID $entityId score ${prediction.getScore} states ${prediction.getStates.mkString(",")}")
        counter.add(prediction.getScore.toLong)

        (entityId, prediction.getScore, prediction.getStates.mkString(","), timestamp)
      }})

      val endTime = System.nanoTime()
      val latency = endTime - startTime // Measure the time it took to process the data
      Log.log.warn(s"[Predictor] latency: $latency")

      val elapsedTime = (endTime - startTime) / 1000000000.0
      val mbs: Double = (counter.count / elapsedTime).toDouble
//      val formattedMbs = String.format("%.5f", mbs)
//      Log.log.warn(s"[Predictor] bandwidth: $formattedMbs MB/s")

      lines
    })


  }

}
