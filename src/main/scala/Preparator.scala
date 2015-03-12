package org.template.complementarypurchase

import io.prediction.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  @transient lazy val logger = Logger[this.type]

  def prepare(sc: SparkContext, td: TrainingData): PreparedData = {
    new PreparedData(buyEvents = td.buyEvents)
  }
}

class PreparedData(
  val buyEvents: RDD[BuyEvent]
) extends Serializable
