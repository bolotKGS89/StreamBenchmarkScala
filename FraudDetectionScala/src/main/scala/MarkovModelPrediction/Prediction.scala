package MarkovModelPrediction

import java.io.Serializable


class Prediction(var entityId: String, var score: Double, var states: Array[String], var outlier: Boolean) extends Serializable {
  def getEntityId: String = entityId

  def setEntityId(entityId: String): Unit = {
    this.entityId = entityId
  }

  def getScore: Double = score

  def setScore(score: Double): Unit = {
    this.score = score
  }

  def getStates: Array[String] = states

  def setStates(states: Array[String]): Unit = {
    this.states = states
  }

  def isOutlier: Boolean = outlier

  def setOutlier(outlier: Boolean): Unit = {
    this.outlier = outlier
  }
}
