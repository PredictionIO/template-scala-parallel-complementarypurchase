package org.template.complementarypurchase

import org.apache.predictionio.controller.EngineParamsGenerator
import org.apache.predictionio.controller.EngineParams

trait BaseEngineParamsList extends EngineParamsGenerator {
  protected val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(
      appName = "techselector_v3",
      evalParams = Some(DataSourceEvalParams(kFold = 5, queryNum = 10))))
}

case class Precision(label: Double)
  extends OptionAverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  def calculate(query: Query, predicted: PredictedResult, actual: ActualResult)
  : Option[Double] = {
    if (predicted.label == label) {
      if (predicted.label == actual.label) {
        Some(1.0)  // True positive
      } else {
        Some(0.0)  // False positive
      }
    } else {
      None  // Unrelated case for calcuating precision
    }
  }
}