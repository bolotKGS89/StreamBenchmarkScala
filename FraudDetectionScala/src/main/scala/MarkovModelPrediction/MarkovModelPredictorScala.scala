/** ************************************************************************************
 * Copyright (c) 2019- Gabriele Mencagli and Alessandra Fais
 *
 * This file is part of StreamBenchmarks.
 *
 * StreamBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 * You can redistribute it and/or modify it under the terms of the
 * * GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version
 * OR
 * * MIT License: https://github.com/ParaGroup/StreamBenchmarks/blob/master/LICENSE.MIT
 *
 * StreamBenchmarks is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License and
 * the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 * and <http://opensource.org/licenses/MIT/>.
 * *************************************************************************************
 */

package MarkovModelPrediction

import Constants.FraudDetectionConstants
import Util.config.Configuration
import Util.data.Pair
import org.apache.commons.lang.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.util


/**
 * Predictor based on markov model
 *
 * @author pranab
 */
object MarkovModelPredictor {
  private val LOG = LoggerFactory.getLogger(classOf[MarkovModelPredictorJava])

  object DetectionAlgorithm extends Enumeration {
    type DetectionAlgorithm = Value
    val MissProbability, MissRate, EntropyReduction = Value
  }
}

class MarkovModelPredictorScala extends ModelBasedPredictor with Serializable {
  val conf = new Configuration
  val mmKey: String = conf.getString(FraudDetectionConstants.MARKOV_MODEL_KEY, null)
  var model: String = null
  if (StringUtils.isBlank(mmKey)) model = new MarkovModelResourceSource().getModel(FraudDetectionConstants.DEFAULT_MODEL)
  else model = new MarkovModelFileSource().getModel(mmKey)
  markovModel = new MarkovModel(model)
  localPredictor = conf.getBoolean(FraudDetectionConstants.LOCAL_PREDICTOR)
  if (localPredictor) stateSeqWindowSize = conf.getInt(FraudDetectionConstants.STATE_SEQ_WIN_SIZE)
  else {
    stateSeqWindowSize = 5
    globalParams = new util.HashMap[String, Pair[Double, Double]]
  }
  //state value ordinal within record
  stateOrdinal = conf.getInt(FraudDetectionConstants.STATE_ORDINAL)
  //detection algoritm
  val algorithm: String = conf.getString(FraudDetectionConstants.DETECTION_ALGO)

//  MarkovModelPredictor.LOG.debug("[predictor] detection algorithm: " + algorithm)

  if (algorithm == "missProbability") detectionAlgorithm = MarkovModelPredictor.DetectionAlgorithm.MissProbability
  else if (algorithm == "missRate") {
    detectionAlgorithm = MarkovModelPredictor.DetectionAlgorithm.MissRate
    //max probability state index
    maxStateProbIndex = new Array[Int](markovModel.getNumStates)
//    for (i <- 0 until markovModel.getNumStates) {
//      var maxProbIndex = -1
//      var maxProb = -1
//      for (j <- 0 until markovModel.getNumStates) {
//        if (markovModel.getStateTransitionProb(i)(j) > maxProb) {
//          maxProb = markovModel.getStateTransitionProb(i)(j)
//          maxProbIndex = j
//        }
//      }
//      maxStateProbIndex(i) = maxProbIndex
//    }
  }
  else if (algorithm == "entropyReduction") {
//    detectionAlgorithm = MarkovModelPredictor.DetectionAlgorithm.EntropyReduction
//    //entropy per source state
//    entropy = new Array[Double](markovModel.getNumStates)
//    for (i <- 0 until markovModel.getNumStates) {
//      var ent = 0
//      for (j <- 0 until markovModel.getNumStates) {
//        ent += -markovModel.getStateTransitionProb(i)(j) * Math.log(markovModel.getStateTransitionProb(i)(j))
//      }
//      entropy(i) = ent
//    }
  }
  else {
    //error
    val msg = "The detection algorithm '" + algorithm + "' does not exist"
//    MarkovModelPredictor.LOG.error(msg)
    throw new RuntimeException(msg)
  }
  //metric threshold
  metricThreshold = conf.getDouble(FraudDetectionConstants.METRIC_THRESHOLD)
//  MarkovModelPredictor.LOG.debug("[predictor] the threshold is: " + metricThreshold)
  private var markovModel: MarkovModel = null
  final private val records = new util.HashMap[String, util.List[String]]
  private var localPredictor = false
  private var stateSeqWindowSize = 0
  private var stateOrdinal = 0
  private var detectionAlgorithm: MarkovModelPredictor.DetectionAlgorithm.DetectionAlgorithm = null
  private var globalParams: util.Map[String, Pair[Double, Double]] = null
  private var metricThreshold = .0
  private var maxStateProbIndex: Array[Int] = null
  private var entropy: Array[Double] = null

  override def execute(entityID: String, record: String): Prediction = {
    var score = 0
    var recordSeq = records.get(entityID)
    if (null == recordSeq) {
      recordSeq = new util.ArrayList[String]
      records.put(entityID, recordSeq)
    }
    //add and maintain size
    recordSeq.add(record)
    if (recordSeq.size > stateSeqWindowSize) recordSeq.remove(0)
    var stateSeq: Array[String] = null
    if (localPredictor) {
      //local metric
//      MarkovModelPredictor.LOG.debug("local metric,  seq size " + recordSeq.size)
      if (recordSeq.size == stateSeqWindowSize) {
        stateSeq = new Array[String](stateSeqWindowSize)
        for (i <- 0 until stateSeqWindowSize) {
          stateSeq(i) = recordSeq.get(i).split(",")(stateOrdinal)
//          MarkovModelPredictor.LOG.debug("[predictor] size={}, stateseq[{}][1]={}", recordSeq.size, i, stateSeq(i))
        }
        score = getLocalMetric(stateSeq).toInt
      }
    }
    else {
      //global metric
//      MarkovModelPredictor.LOG.debug("global metric")
      if (recordSeq.size >= 2) {
        stateSeq = new Array[String](2)
        var i = stateSeqWindowSize - 2
        var j = 0
        while (i < stateSeqWindowSize) {
          stateSeq({
            j += 1; j - 1
          }) = recordSeq.get(i).split(",")(stateOrdinal)

          i += 1
        }
        var params = globalParams.get(entityID)
        if (null == params) {
          params = new Pair[Double, Double](0.0, 0.0)
          globalParams.put(entityID, params)
        }
        score = getGlobalMetric(stateSeq, params)
      }
    }
    //outlier
//    MarkovModelPredictor.LOG.debug("outlier: metric " + entityID + ":" + score)
    val prediction = new Prediction(entityID, score, stateSeq, score > metricThreshold)
    if (score > metricThreshold) {
    }
    prediction
  }

  /**
   * @param stateSeq
   * @return
   */
  private def getLocalMetric(stateSeq: Array[String]): Int = {
    var metric = 0
    val params = new Array[Double](2)
    params(0) = 0
    params(1) = 0
    if (detectionAlgorithm == MarkovModelPredictor.DetectionAlgorithm.MissProbability) missProbability(stateSeq, params)
    else if (detectionAlgorithm eq MarkovModelPredictor.DetectionAlgorithm.MissRate) missRate(stateSeq, params)
    else entropyReduction(stateSeq, params)
    metric = (params(0) / params(1)).toInt
    metric
  }

  /**
   * @param stateSeq
   * @return
   */
  private def getGlobalMetric(stateSeq: Array[String], globParams: Pair[Double, Double]) = {
    var metric = 0
    var params = new Array[Double](2)
    params(0) = 0
    params(1) = 0d
    if (detectionAlgorithm eq MarkovModelPredictor.DetectionAlgorithm.MissProbability) {
      missProbability(stateSeq, params)
    }
    else if (detectionAlgorithm eq MarkovModelPredictor.DetectionAlgorithm.MissRate) missRate(stateSeq, params)
    else entropyReduction(stateSeq, params)
    globParams.setLeft(globParams.getLeft + params(0))
    globParams.setRight(globParams.getRight + params(1))
    metric = (globParams.getLeft / globParams.getRight).toInt
    metric
  }

  /**
   * @param stateSeq
   * @return
   */
  private def missProbability(stateSeq: Array[String], params: Array[Double]): Unit = {
//    val start = if (localPredictor) 1
//    else stateSeq.length - 1
//    for (i <- start until stateSeq.length) {
//      val prState = markovModel.getStates.indexOf(stateSeq(i - 1))
//      val cuState = markovModel.getStates.indexOf(stateSeq(i))
////      MarkovModelPredictor.LOG.debug("state prob index:" + prState + " " + cuState)
//      //add all probability except target state
//      for (j <- 0 until markovModel.getStates.size) {
//        if (j != cuState) params(0) += markovModel.getStateTransitionProb(prState)(j)
//      }
//      params(1) += 1
//    }
////    MarkovModelPredictor.LOG.debug("params:" + params(0) + ":" + params(1))
  }

  /**
   * @param stateSeq
   * @return
   */
  private def missRate(stateSeq: Array[String], params: Array[Double]): Unit = {
    val start = if (localPredictor) 1
    else stateSeq.length - 1
    for (i <- start until stateSeq.length) {
      val prState = markovModel.getStates.indexOf(stateSeq(i - 1))
      val cuState = markovModel.getStates.indexOf(stateSeq(i))
      params(0) += (if (cuState == maxStateProbIndex(prState)) 0
      else 1)
      params(1) += 1
    }
  }

  /**
   * @param stateSeq
   * @return
   */
  private def entropyReduction(stateSeq: Array[String], params: Array[Double]): Unit = {
    val start = if (localPredictor) 1
    else stateSeq.length - 1
    for (i <- start until stateSeq.length) {
      val prState = markovModel.getStates.indexOf(stateSeq(i - 1))
      val cuState = markovModel.getStates.indexOf(stateSeq(i))
      params(0) += (if (cuState == maxStateProbIndex(prState)) 0
      else 1)
      params(1) += 1
    }
  }
}