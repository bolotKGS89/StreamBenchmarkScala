package MarkovModelPrediction

trait IMarkovModelSource {
  def getModel(key: String): String
}
