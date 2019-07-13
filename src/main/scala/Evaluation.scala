package org.template.complementarypurchase

import org.apache.predictionio.controller.EngineParamsGenerator
import org.apache.predictionio.controller.EngineParams
// Usage:
// $ pio eval org.template.complementarypurchase.ComplementaryPurchaseEvaluation \
//   org.template.complementarypurchase.EngineParamsList

case class PrecisionAtK(k: Int, ratingThreshold: Double = 2.0)
    extends OptionAverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  require(k > 0, "k must be greater than 0")

  override def header = s"Precision@K (k=$k, threshold=$ratingThreshold)"

  override
  def calculate(q: Query, p: PredictedResult, a: ActualResult): Option[Double] = {
    val positives: Set[String] = a.ratings.filter(_.rating >= ratingThreshold).map(_.item).toSet

    // If there is no positive results, Precision is undefined. We don't consider this case in the
    // metrics, hence we return None.
    if (positives.size == 0) {
      None
    } else {
      val tpCount: Int = p.itemScores.take(k).filter(is => positives(is.item)).size
      Some(tpCount.toDouble / math.min(k, positives.size))
    }
  }
}
object ComplementaryPurchaseEvaluation extends Evaluation {
  engineEvaluator = (
    RecommendationEngine(),
    MetricEvaluator(
      metric = PrecisionAtK(k = 10, ratingThreshold = 4.0),
      otherMetrics = Seq(
        PositiveCount(ratingThreshold = 4.0),
        PrecisionAtK(k = 10, ratingThreshold = 2.0),
        PositiveCount(ratingThreshold = 2.0),
        PrecisionAtK(k = 10, ratingThreshold = 1.0),
        PositiveCount(ratingThreshold = 1.0)
      )))
}
trait BaseEngineParamsList extends EngineParamsGenerator {
  protected val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(
      appName = "techselector_v3",
      evalParams = Some(DataSourceEvalParams(kFold = 5, queryNum = 10))))
}
