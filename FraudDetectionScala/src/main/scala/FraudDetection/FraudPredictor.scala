package FraudDetection

import MarkovModelPrediction.{MarkovModelPredictorJava, ModelBasedPredictor, Prediction}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{State, StateSpec, StreamingContext}

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable


case class State(recordSeq: ListBuffer[String], records: Map[String, ListBuffer[String]])

class FraudPredictor extends Serializable {

// create a function to update the state variables
//  private def updateState(entityId: String, record: String, timestamp: Long, state: Option[State]): (String, Prediction, Long, State) = {
//    // create the initial state if it does not exist
//    val s = state.getOrElse(State(new ListBuffer[String], Map()))
//
//    val predictor = new MarkovModelPredictorJava()
//
//    // add the record to the recordSeq for the entityId
//    s.recordSeq += record
//    if (s.recordSeq.size > 5) {
//      s.recordSeq.remove(0)
//      println(s"entityId $entityId")
//    }
//
//    // update the records map
//    val updatedRecords = s.records.updated(entityId, s.recordSeq)
//
//    // create a new state object with the updated variables
//    val newState = State(s.recordSeq, updatedRecords)
//
//    // execute the predictor on the recordSeq for the entityId
//    val p = predictor.execute(entityId, record, s.recordSeq.size)
//
//    // return the result and the new state object
//    (entityId, p, timestamp, newState)
//  }


  def execute(lines: DStream[(String, String, Long)], ssc: StreamingContext, predModel: String, predictorParDeg: Int):
  DStream[(String, Double, String, Long)] = {

      // create the initial state RDD with empty state objects
//      val initialStateRDD = ssc.sparkContext.parallelize(Seq((0L, State(new ListBuffer[String], Map()))))
//
//      val stateSpec = StateSpec.function[(Long, Seq[(String, Long)]), State, (Long, Seq[(String, Long)])](updateState _)
//        .initialState(initialStateRDD)
//
//      // create the stateful DStream with mapWithState transformation
//      val statefulRDD = lines.repartition(predictorParDeg)
//        .map { case (entityId, record, timestamp) => (entityId, (record, timestamp)) }
//        .groupByKey()
//        .mapWithState(stateSpec)
//
//      // filter the outliers and map to the desired output format
//      val filteredRDD = statefulRDD.filter { case (_, p, _) => p.isOutlier }
//        .map { case (entityId, p, timestamp, _) => (entityId, p.getScore, p.getStates.mkString(","), timestamp) }
//
//      filteredRDD

      val counter = ssc.sparkContext.longAccumulator("Predictor accumulator")



      val cachedRDD = lines.repartition(predictorParDeg).map { case (entityId, record, timestamp) =>
        (entityId, (record, timestamp))
      }.groupByKey()
        .flatMap { itr =>
          itr._2.map { case (record, timestamp) =>
            var predictor: ModelBasedPredictor = null

            if (predModel.equals("mm")) {
              predictor = new MarkovModelPredictorJava()
            }
            // add record to recordSeq
            val records = new mutable.ParHashMap[String, ListBuffer[String]]
            var recordSeq: ListBuffer[String] = records.getOrElse(itr._1, new ListBuffer[String])
//              .getOrElseUpdate(itr._1, new ListBuffer[String])
            recordSeq += record
            if (recordSeq.size > 5) {
              recordSeq.remove(0)
              System.out.println("entityId " + itr._1)
            }

            val p = predictor.execute(itr._1, record)

            (itr._1, p, timestamp)
          }
        }.persist(StorageLevel.DISK_ONLY)

      val filteredRDD = cachedRDD.filter { case (_, p, _) => p.isOutlier }
        .map { case (entityId, p, timestamp) =>
          (entityId, p.getScore, p.getStates.mkString(","), timestamp)
        }

      filteredRDD

  }
}



//      .transform({ rdd =>


//      val lines = rdd
//        .repartition(predictorParDeg)
//
//
//        .map { case (entityId, record, timestamp) =>
//
//          var recordSeq: ListBuffer[String] = records.getOrElseUpdate(entityId, new ListBuffer[String])
//          recordSeq += record
//          if (recordSeq.size > 5) {
//            recordSeq.remove(0)
//          }
//
//          val p = predictor.execute(entityId, record)
//
//          if (p.isOutlier) System.out.println("entityId " + entityId + " score " + p.getScore +
//            " states " + p.getStates.mkString(",") + " ts " + timestamp)
//
//          (entityId, p, timestamp)
//        }.cache
//        .filter { case (_, p, _) => p.isOutlier }
//        .map { case (entityId, p, timestamp) =>
//            (entityId, p.getScore, p.getStates.mkString(","), timestamp)
//        }
//      })


//      val endTime = System.nanoTime()
//      val latency = endTime - startTime // Measure the time it took to process the data
//      Log.log.warn(s"[Predictor] latency: $latency")
//
//      val elapsedTime = (endTime - startTime) / 1000000000.0
//      val mbs: Double = (counter.count / elapsedTime).toDouble
//      val formattedMbs = String.format("%.5f", mbs)
//      Log.log.warn(s"[Predictor] bandwidth: $formattedMbs MB/s")
//  }
//
//}
