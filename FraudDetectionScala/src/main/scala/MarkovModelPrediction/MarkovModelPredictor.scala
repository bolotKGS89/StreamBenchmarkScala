package MarkovModelPrediction

import Constants.FraudDetectionConstants
import Constants.FraudDetectionConstants.Conf.LOCAL_PREDICTOR
import Constants.FraudDetectionConstants.DEFAULT_MODEL
import Util.Log
import Util.data.Pair

import java.util
import java.util.{HashMap, List, Map, Properties}

class MarkovModelPredictor(mmKey: String) extends ModelBasedPredictor {
  private object DetectionAlgorithm extends Enumeration {
    type DetectionAlgorithm = Value
    val MissProbability, MissRate, EntropyReduction = Value
  }

  private var markovModel: MarkovModel = null

  private val records: Map[String, List[String]] = new HashMap[String, List[String]]
  private var localPredictor = false
  private var stateSeqWindowSize = 0
  private var stateOrdinal = 0
  private var detectionAlgorithm: DetectionAlgorithm.Value = null
  private var metricThreshold = .0
  private var maxStateProbIndex: Array[Int] = null
  private var entropy: Array[Double] = null
  private var algorithm: String = null

  private var globalParams: java.util.Map[String, Pair[Double, Double]] = new HashMap[String, Pair[Double, Double]]()
  private var model: String = null

  if(mmKey.isEmpty) {
    model = new MarkovModelResourceSource().getModel(FraudDetectionConstants.DEFAULT_MODEL)
  } else {
    model = new MarkovModelFileSource().getModel(mmKey)
  }

  val props = new Properties()
  val resourceStream = getClass.getResourceAsStream("/fd.properties")
  props.load(resourceStream)

  localPredictor = props.getProperty(FraudDetectionConstants.Conf.LOCAL_PREDICTOR).toBoolean


  markovModel = new MarkovModel(model)
  this.localPredictor = true

  if (localPredictor) stateSeqWindowSize = props.getProperty(FraudDetectionConstants.Conf.STATE_SEQ_WIN_SIZE).toInt
  else {
    stateSeqWindowSize = 5
    globalParams = new util.HashMap[String, Pair[Double, Double]]
  }

  //state value ordinal within record
  stateOrdinal = props.getProperty(FraudDetectionConstants.Conf.STATE_ORDINAL).toInt

  //detection algoritm
  algorithm = props.getProperty(FraudDetectionConstants.Conf.DETECTION_ALGO).toString
  Log.log.debug(s"[predictor] detection algorithm: $algorithm")


  if (algorithm == "missProbability") detectionAlgorithm = DetectionAlgorithm.MissProbability
  else if (algorithm == "missRate") {
    detectionAlgorithm = DetectionAlgorithm.MissRate
    //max probability state index
    maxStateProbIndex = new Array[Int](markovModel.getNumStates)
    for (i <- 0 until markovModel.getNumStates) {
      var maxProbIndex = -1
      var maxProb: Double = -1
      for (j <- 0 until markovModel.getNumStates) {
        if (markovModel.getStateTransitionProb(i)(j) > maxProb) {
          maxProb = markovModel.getStateTransitionProb(i)(j)
          maxProbIndex = j
        }
      }
      maxStateProbIndex(i) = maxProbIndex
    }
  }
  else if (algorithm == "entropyReduction") {
    detectionAlgorithm = DetectionAlgorithm.EntropyReduction
    //entropy per source state
    entropy = new Array[Double](markovModel.getNumStates)
    for (i <- 0 until markovModel.getNumStates) {
      var ent: Double = 0
      for (j <- 0 until markovModel.getNumStates) {
        ent += -markovModel.getStateTransitionProb(i)(j) * Math.log(markovModel.getStateTransitionProb(i)(j))
      }
      entropy(i) = ent
    }
  }
  else { //error
    val msg = s"The detection algorithm $algorithm does not exist"
    Log.log.error(msg)
    throw new RuntimeException(msg)
  }



  //metric threshold
  metricThreshold = props.getProperty(FraudDetectionConstants.Conf.METRIC_THRESHOLD).toInt
  Log.log.debug(s"[predictor] the threshold is: $metricThreshold")


  override def execute(entityID: String, record: String): Prediction = {
    var score: Double = 0
    var recordSeq = records.get(entityID)
    if (null == recordSeq) {
      recordSeq = new util.ArrayList[String]
      records.put(entityID, recordSeq)
    }
    //add and maintain size/home/hdoop/dataset/fd
    recordSeq.add(record)
    if (recordSeq.size > stateSeqWindowSize) recordSeq.remove(0)
    var stateSeq: Array[String] = null
    if (localPredictor) { //local metric
      if (recordSeq.size == stateSeqWindowSize) {
        stateSeq = new Array[String](stateSeqWindowSize)
        for (i <- 0 until stateSeqWindowSize) {
          stateSeq(i) = recordSeq.get(i).split(",")(stateOrdinal)
        }
        score = getLocalMetric(stateSeq)
      }
    }
    else { //global metric
      if (recordSeq.size >= 2) {
        stateSeq = new Array[String](2)
        var i = stateSeqWindowSize - 2
        var j = 0
        while ( {
          i < stateSeqWindowSize
        }) {
          stateSeq({
            j += 1
            j - 1
          }) = recordSeq.get(i).split(",")(stateOrdinal)

          i = i + 1
        }
        var params = globalParams.get(entityID)
        if (null == params) {
          params = new Pair[Double, Double](0.0, 0.0)
          globalParams.put(entityID, params)
        }
        score = getGlobalMetric(stateSeq, params)
      }
    }

    val prediction = new Prediction(entityID, score, stateSeq, score > metricThreshold)
    if (score > metricThreshold) {
      /*
                  StringBuilder stBld = new StringBuilder(entityID);
                  stBld.append(" : ");
                  for (String st : stateSeq) {
                      stBld.append(st).append(" ");
                  }
                  stBld.append(": ");
                  stBld.append(score);
                  jedis.lpush(outputQueue,  stBld.toString());
                  */
      // should return the score and state sequence
      // should say if is an outlier or not
    }
    prediction
  }

  private def getLocalMetric(stateSeq: Array[String]) = {
    val params = new Array[Double](2)
    params(0) = 0d
    params(1) = 0d
    if (detectionAlgorithm eq DetectionAlgorithm.MissProbability) missProbability(stateSeq, params)
    else if (detectionAlgorithm eq DetectionAlgorithm.MissRate) missRate(stateSeq, params)
    else entropyReduction(stateSeq, params)
    val metric = params(0) / params(1)
    metric
  }

  private def getGlobalMetric(stateSeq: Array[String], globParams: Pair[Double, Double]) = {
    var metric : Double = 0
    val params = new Array[Double](2)
    params(0) = 0
    params(1) = 0
    if (detectionAlgorithm eq DetectionAlgorithm.MissProbability) missProbability(stateSeq, params)
    else if (detectionAlgorithm eq DetectionAlgorithm.MissRate) missRate(stateSeq, params)
    else entropyReduction(stateSeq, params)
    globParams.setLeft(globParams.getLeft + params(0))
    globParams.setRight(globParams.getRight + params(1))
    metric = globParams.getLeft / globParams.getRight
    metric
  }

  private def missProbability(stateSeq: Array[String], params: Array[Double]): Unit = {
    val start = if (localPredictor) 1 else stateSeq.length - 1
    for (i <- start until stateSeq.length) {
      val prState = markovModel.getStates.indexOf(stateSeq(i - 1))
      val cuState = markovModel.getStates.indexOf(stateSeq(i))
      //add all probability except target state
      for (j <- 0 until markovModel.getStates.size) {
        if (j != cuState) params(0) += markovModel.getStateTransitionProb(prState)(j)
      }
      params(1) += 1
    }
  }

  private def missRate(stateSeq: Array[String], params: Array[Double]): Unit = {
    val start = if (localPredictor) 1
    else stateSeq.length - 1
    for (i <- start until stateSeq.length) {
      val prState = markovModel.getStates.indexOf(stateSeq(i - 1))
      val cuState = markovModel.getStates.indexOf(stateSeq(i))
      params(0) = params(0) + (if (cuState == maxStateProbIndex(prState)) 0 else 1)
      params(1) = params(1) + 1
    }
  }

  private def entropyReduction(stateSeq: Array[String], params: Array[Double]): Unit = {
    val start = if (localPredictor) 1
    else stateSeq.length - 1
    for (i <- start until stateSeq.length) {
      val prState = markovModel.getStates.indexOf(stateSeq(i - 1))
      val cuState = markovModel.getStates.indexOf(stateSeq(i))
      params(0) = params(0) + (if (cuState == maxStateProbIndex(prState)) 0 else 1)
      params(1) += 1
    }
  }

}