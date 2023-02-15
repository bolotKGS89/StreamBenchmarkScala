package FraudDetection

import MarkovModelPrediction.{MarkovModelPredictor, ModelBasedPredictor}
import org.apache.spark.streaming.dstream.DStream
import Util.Log
import org.apache.commons.lang.StringUtils
import org.apache.spark.streaming.StreamingContext

class FraudPredictor extends Serializable {

  private var predictor: ModelBasedPredictor = null;

  def execute(lines: DStream[(String, String, Long)], ssc: StreamingContext, predModel: String): DStream[(String, Double, String, Long)] = {

    val counter = ssc.sparkContext.longAccumulator("Predictor accumulator")

    lines.transform({ rdd =>
      val startTime = System.nanoTime()
      val lines = rdd.map((lines) => {
        val entityId = lines._1
        val record = lines._2
        val timestamp = lines._3

        val strategy = "mm"
        if (strategy.eq(predModel)) {
          predictor = new MarkovModelPredictor(strategy)
        }

        val p = predictor.execute(entityId, record)
        Log.log.debug(s"[Predictor] tuple: entityID $entityId record $record ts $timestamp")

        (p, entityId, timestamp)
      }).filter((predTuple) => {
        val prediction = predTuple._1
        prediction.isOutlier
      }).map((predTuple) => {
        val entityId = predTuple._2
        val score = predTuple._1.getScore
        val states = predTuple._1.getStates.mkString(",")
        val timeStamp = predTuple._3
        Log.log.debug(s"[Predictor] outlier: entityID $entityId score $score states $states")
        counter.add(score.toLong)

        (entityId, score, states, timeStamp)
      })

      val endTime = System.nanoTime()
      val latency = endTime - startTime // Measure the time it took to process the data
      Log.log.warn(s"[Predictor] latency: $latency")

      val elapsedTime = (endTime - startTime) / 1000000000.0
      val mbs: Double = (counter.count / elapsedTime).toDouble
      val formattedMbs = String.format("%.5f", mbs)
      Log.log.warn(s"[Predictor] bandwidth: $formattedMbs MB/s")

      lines
    })


  }

}
