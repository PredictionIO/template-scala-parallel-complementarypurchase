package org.template.complimentarypurchase

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class AlgorithmParams(
  basketWindow: Int, // in seconds
  minSupport: Int,
  //threshold: Double,
  //maxSupport: Int,
  topNum: Int) extends Params

class Algorithm(val ap: AlgorithmParams)
  // extends PAlgorithm if Model contains RDD[]
  extends P2LAlgorithm[PreparedData, Model, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(pd: PreparedData): Model = {
    val windowMillis = ap.basketWindow * 1000
    // item Support
    val itemSupport: Map[String, Int] = pd.buyEvents
      .map(b => (b.item, 1))
      .reduceByKey((a, b) => a + b)
      .collectAsMap.toMap

    val totalItem = itemSupport.size
    logger.info(s"itemSupport: ${itemSupport}. size ${totalItem}")

    val set2Support: RDD[(Set[String], Int)] = pd.buyEvents
      .map (b => (b.user, new ItemAndTime(b.item, b.t)))
      .groupByKey
      // create RDD[Set[String]] // size 2
      .flatMap{ case (user, iter) => // user and iterable of ItemAndTime
        // sort by time and create List[ItemSet] based on time and window
        val sortedList = iter.toList.sortBy(_.t)
        val init = ItemSet[String](Set(sortedList.head.item), sortedList.head.t)
        val userItemSets = sortedList.tail
          .foldLeft(List[ItemSet[String]](init)) ( (list, itemAndTime) =>
            // if current item time is within last item's time's window
            // add to same set.
            if ((itemAndTime.t - list.head.lastTime) <= windowMillis)
              (list.head + itemAndTime) :: list.tail
            else
              ItemSet(Set(itemAndTime.item), itemAndTime.t) :: list
          )
          logger.info(s"user ${user}: ${userItemSets}.")
          // TODO: filter ItemSet with size < 2
        userItemSets.flatMap{ itemSet =>
          itemSet.items.subsets(2).toList
        }
      }
      // map to RDD[Set[T]]
      .map { s => (s, 1) }
      .reduceByKey((a, b) => a + b )
      .filter{ case (set, support) => support >= ap.minSupport}
      .cache()

    logger.info(s"setSupport: ${set2Support.collect.toList}")

    val scores = set2Support
      .flatMap { case (set, support) =>
        val vec = set.toVector
        val a = vec(0)
        val b = vec(1)
        val lift = support.toDouble * totalItem /
          (itemSupport(a) * itemSupport(b))
        Vector((a, (b, lift)), (b, (a, lift)))
      }
      .groupByKey
      .map { case (item, iter) => // iterable[(String, Double)]
        val vec = iter.toVector
          .sortBy(_._2)(Ordering.Double.reverse)
          .take(ap.topNum)
        (item, vec)
      }
      .collectAsMap.toMap

    new Model(scores) // TODO
  }

  def predict(model: Model, query: Query): PredictedResult = {
    val combined = query.items
      .flatMap { i =>
        model.scores.get(i).getOrElse{
          logger.info(s"Item ${i} not found in model.")
          Vector[(String, Double)]()
        }
      }
      .groupBy(_._1)
      // List if (String, Double)
      .mapValues( list => list.map(t => t._2).reduce( (a, b) => a + b) )
      .toArray
      .sortBy(t => t._2)(Ordering.Double.reverse)
      .take(query.num)
      .map { case (k, v) => ItemScore(k, v) }
    new PredictedResult(combined)
  }

  // item and time
  case class ItemAndTime[T](item: T, t: Long)

  // item set with last time of item
  case class ItemSet[T](items: Set[T], lastTime: Long) {
    def size = items.size

    def isEmpty = items.isEmpty

    def +(elem: ItemAndTime[T]): ItemSet[T] = {
      val newSet = items + elem.item
      val newLastTime = if (elem.t > lastTime) elem.t else lastTime
      new ItemSet(newSet, newLastTime)
    }
  }
}

class Model(
  val scores: Map[String, Vector[(String, Double)]]
) extends Serializable {
  override def toString = s"scores: ${scores.size} ${scores.take(2)}..."
}
