package org.template.complementarypurchase

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceEvalParams(kFold: Int, queryNum: Int)
case class DataSourceParams(appName: String, evalParams: Option[DataSourceEvalParams]) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    val buyEvents: RDD[BuyEvent] = PEventStore.find(
      appName = dsp.appName,
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
 override
   def readEval(sc: SparkContext)
   : Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {
     require(!dsp.evalParams.isEmpty, "Must specify evalParams")
     val evalParams = dsp.evalParams.get

     val kFold = evalParams.kFold
     // val ratings: RDD[(Rating, Long)] = getRatings(sc).zipWithUniqueId
     //ratings.cache

     (0 until kFold).map { idx => {

       // val trainingRatings = ratings.filter(_._2 % kFold != idx).map(_._1)  no ratings, so commented
       // val testingRatings = ratings.filter(_._2 % kFold == idx).map(_._1) no ratings, so commented

       // I guess here we have to compare predicted with actual, but i dont know exactly how.

       // I think here should be the business logic, for that:
       // Quote Kenneth: instead of filter by high rating item,
       // as long as the items bought together given the input item,
       //you treat it as positive actual results and compare with the predicted result.
       // Not sure how to imlement this. Some hints?

       val testingUsers: RDD[(String, Iterable[Rating])] = testingRatings.groupBy(_.user)

       (new TrainingData(trainingRatings),
         new EmptyEvaluationInfo(),
         testingUsers.map {
           case (user, ratings) => (Query(user, evalParams.queryNum), ActualResult(ratings.toArray))
         }
       )
     }}
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
