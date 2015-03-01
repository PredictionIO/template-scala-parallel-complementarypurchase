package org.template.complimentarypurchase

import io.prediction.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, td: TrainingData): PreparedData = {
    new PreparedData(buyEvents = td.buyEvents)
  }
}

class PreparedData(
  val buyEvents: RDD[BuyEvent]
) extends Serializable
