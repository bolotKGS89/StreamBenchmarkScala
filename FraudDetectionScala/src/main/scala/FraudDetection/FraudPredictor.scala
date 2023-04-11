package FraudDetection

import MarkovModelPrediction.{MarkovModelPredictorJava, ModelBasedPredictor}
import org.apache.spark.streaming.dstream.DStream
import Util.Log
import Util.config.Configuration
import org.apache.hadoop.fs.Path
import org.apache.commons.lang.StringUtils
import org.apache.spark.streaming.StreamingContext

class FraudPredictor extends Serializable {

  def execute(lines: DStream[(String, String, Long)], ssc: StreamingContext, predModel: String, predictorParDeg: Int):
  DStream[(String, Double, String, Long)] = {

    val counter = ssc.sparkContext.longAccumulator("Predictor accumulator")

    var predictor: ModelBasedPredictor = null;

    if (predModel.equals("mm")) {
      predictor = new MarkovModelPredictorJava()
    }


    lines.repartition(predictorParDeg).transform({ rdd =>

      val startTime = System.nanoTime()

      val lines = rdd
          .repartition(predictorParDeg).groupBy(_._1).flatMap { iter =>
            iter._2.map { case (entityId, record, timestamp) =>
              val p = predictor.execute(entityId, record)
              (p, entityId, timestamp)
            }.filter { case (prediction, entityId, timestamp) =>
              prediction.isOutlier
            }.map { case (prediction, entityId, timestamp) =>
              (entityId, prediction.getScore, prediction.getStates.mkString(","), timestamp)
            }
          }
//        .map { input =>
//          val entityID = input._1
//          val record = input._2
//          val timestamp = input._3
//          val p = predictor.execute(entityID, record)
//          (entityID, p, timestamp)
//        }.filter { case (_, p, _) => p.isOutlier }
//        .map { case (entityID, p, timestamp) =>
//          (entityID, p.getScore, p.getStates.mkString(","), timestamp)
//        }

      lines
      })


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
