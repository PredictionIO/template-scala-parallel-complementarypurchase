package org.template.complementarypurchase

import org.apache.predictionio.controller.Evaluation
import org.apache.predictionio.controller.EngineParamsGenerator
import org.apache.predictionio.controller.EngineParams
import org.apache.predictionio.controller.MetricEvaluator
import org.apache.predictionio.controller.OptionAverageMetric

// Usage:
// $ pio eval org.template.complementarypurchase.ComplementaryPurchaseEvaluation \
//   org.template.complementarypurchase.EngineParamsList

case class PrecisionAtK(k: Int)
    extends OptionAverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  require(k > 0, "k must be greater than 0")

  override def header = s"Precision@K (k=$k, threshold=$ratingThreshold)"

  override
  def calculate(q: Query, p: PredictedResult, a: ActualResult): Option[Double] = {
    //val positives: Set[String] = a.ratings.filter(_.rating >= ratingThreshold).map(_.item).toSet

    //Here we should filter by items to find only the ones, which were bought
    //together with the input item, but how should the syntax look like?
    //val positives: Set[String] a.items,filter

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
    ComplementaryPurchaseEngine(),
    MetricEvaluator(
      metric = PrecisionAtK(k = 10)
    )
  )
}

object ComprehensiveRecommendationEvaluation extends Evaluation {

  // val ratingThresholds = Seq(0.0, 2.0, 4.0)
  val ks = Seq(1, 3, 10)

  engineEvaluator = (
    ComplementaryPurchaseEngine(),
    MetricEvaluator(
      metric = PrecisionAtK(k = 3)
    )
  )
}

trait BaseEngineParamsList extends EngineParamsGenerator {
  protected val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(
      appName = "techselector_v3",
      evalParams = Some(DataSourceEvalParams(kFold = 5, queryNum = 10))))
}
