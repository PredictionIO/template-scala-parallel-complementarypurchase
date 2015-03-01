package org.template.complimentarypurchase

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

case class Query(items: Set[String], num: Int)
  extends Serializable

case class PredictedResult(rules: Array[Rule])
  extends Serializable

//case class ItemScore(item: String, score: Double) extends Serializable
case class Rule(cond: Set[String], itemScores: Array[ItemScore])
  extends Serializable

case class ItemScore(
  item: String, support: Double, confidence: Double, lift: Double
) extends Serializable

object ComplimentaryPurchaseEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("algo" -> classOf[Algorithm]),
      classOf[Serving])
  }
}
