package MarkovModelPrediction

abstract class ModelBasedPredictor {
  def execute(entityID: String, record: String): Prediction
}
