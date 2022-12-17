package FraudDetection

import MarkovModelPrediction.{MarkovModelPredictor, ModelBasedPredictor}
import org.apache.spark.streaming.dstream.DStream

class FraudPredictor extends Serializable {

  private var predictor: ModelBasedPredictor = null;

  def execute(lines: DStream[(String, String, Long)]): DStream[(String, Double, String, Long)] = {
    lines.map((lines) => {
      val entityId = lines._1
      val record = lines._2
      val timestamp = lines._3

      val strategy = "mm"
      if (strategy.eq("mm")) {
        predictor = new MarkovModelPredictor(strategy)
      }

      val p = predictor.execute(entityId, record)

      (p, entityId, timestamp)
    }).filter((predTuple) => {
      val prediction = predTuple._1
      prediction.isOutlier
    }).map((predTuple) => {
      val entityId = predTuple._2
      val score = predTuple._1.getScore
      val states = predTuple._1.getStates.mkString(",")
      val timeStamp = predTuple._3
      (entityId, score, states, timeStamp)
    })
  }

}
