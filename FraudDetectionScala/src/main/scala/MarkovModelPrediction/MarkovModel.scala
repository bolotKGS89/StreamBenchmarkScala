package MarkovModelPrediction

import java.util
import java.util.{Arrays, List, Scanner}
import scala.collection.JavaConverters._

class MarkovModel(val model: String) extends Serializable{
  val scanner: Scanner = new Scanner(model)
  var lineCount: Int = 0
  var row: Int = 0
  while ( {
    scanner.hasNextLine
  }) {
    val line: String = scanner.nextLine
    if (lineCount == 0) { //states
      val items = line.split(",")
      states = items.toList.asJava
      numStates = items.length
      stateTransitionProb = Array.ofDim[Double](numStates, numStates)
    }
    else { //populate state transtion probability
      deseralizeTableRow(stateTransitionProb, line, ",", row, numStates)
      row += 1
    }
    lineCount += 1
  }
  scanner.close()
  private var states: util.List[String] = null
  private var stateTransitionProb: Array[Array[Double]] = null
  private var numStates: Int = 0

  /**
   * @param table
   * @param data
   * @param delim
   * @param row
   * @param numCol
   */
  private def deseralizeTableRow(table: Array[Array[Double]], data: String, delim: String, row: Int, numCol: Int): Unit = {
    val items: Array[String] = data.split(delim)
    if (items.length != numCol) throw new IllegalArgumentException("SchemaRecord serialization failed, number of tokens in string does not match with number of columns")
    for (c <- 0 until numCol) {
      table(row)(c) = items(c).toDouble
    }
  }

  def getStates: util.List[String] = states

  def getStateTransitionProb: Array[Array[Double]] = stateTransitionProb

  def getNumStates: Int = numStates
}
