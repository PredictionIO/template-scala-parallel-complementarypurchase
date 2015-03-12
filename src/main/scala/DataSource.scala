package org.template.complementarypurchase

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.storage.Storage

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(appId: Int) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {
    val eventsDb = Storage.getPEvents()

    // get all "user" "buy" "item" events
    val buyEvents: RDD[BuyEvent] = eventsDb.find(
      appId = dsp.appId,
      entityType = Some("user"),
      eventNames = Some(List("buy")),
      targetEntityType = Some(Some("item")))(sc)
      .map { event =>
        try {
          new BuyEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception => {
            logger.error(s"Cannot convert ${event} to BuyEvent. ${e}")
            throw e
          }
        }
      }.cache()

    new TrainingData(buyEvents)
  }
}

case class BuyEvent(user: String, item: String, t: Long)

class TrainingData(
  val buyEvents: RDD[BuyEvent]
) extends Serializable {
  override def toString = {
    s"buyEvents: [${buyEvents.count()}] (${buyEvents.take(2).toList}...)"
  }
}
