package main

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import structure.{ALOCIForest, Element}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream

import scala.math.abs
import scala.collection.mutable


object AppMain {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val SECONDS_OF_WINDOW = 180
  val NUM_OF_TREES = 1
  val ALPHA = 4

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("ALOCI")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))
    val inputData: mutable.Queue[RDD[Element]] = mutable.Queue()
    val inputStream: InputDStream[Element] = ssc.queueStream(inputData)

    val tempDir: String = "./tmp"
    ssc.sparkContext.setCheckpointDir(tempDir: String)

    for (i <- 0 until 100) {
      val inputRDD = ssc.sparkContext.textFile("./resources/input10K/batch_" + i + ".txt", 2)
                          .map(line => line.split(' '))
                          .map(line => Array(line(0).toDouble, line(1).toDouble))
                          .map(vector => new Element(vector, NUM_OF_TREES))
      inputData.enqueue(inputRDD)
    }

    System.out.println("RDDs have been loaded in queue")

    var allElements: RDD[Element] = ssc.sparkContext.emptyRDD[Element]
    val forest: ALOCIForest = new ALOCIForest(ssc.sparkContext, NUM_OF_TREES, Array(0,0), Array(1,1))

    inputStream.foreachRDD(rdd => {

      println("Elements in system: " + allElements.count())
      println("Adding: " + rdd.count())

      // 1. Get new batch of elements in RDD[Element]
      val currentTimeRDD = rdd.map(element => {
        val newElement = element
        newElement.updateTimeStamp
        newElement
      })

      // 2. Update them with their assigned nodes
      val updatedRDD = forest.updateElements(currentTimeRDD)

      // 3. Compute necessary increasing count map
      val nodeCountsToIncr = updatedRDD.flatMap(element => {
        var map: List[((Integer, String), Integer)] = List()
        for (i <- 0 until NUM_OF_TREES) map = map :+ ((i: Integer, element.belongsToNode(i)), 1: Integer)
        map
      }).reduceByKeyLocally(_ + _).toList.groupBy(_._1._1).map(tuple => (tuple._1, tuple._2.map(t => (t._1._2, t._2))))

      // 4. Add counts to nodes
      forest.addElementsToAllTrees(nodeCountsToIncr)

      // 5. Union new elements to allElements
      allElements = allElements.union(updatedRDD)
      allElements.checkpoint()

      // 6. Find expiring elements.
      val currentTime = Timestamp.valueOf(LocalDateTime.now())
      val elementsToDeleteRDD = allElements.filter(element => abs(element.timestamp.getTime - currentTime.getTime) >= SECONDS_OF_WINDOW * 1000)
      println("Deleting: " + elementsToDeleteRDD.count())

      // 7. Filter out from allElements the expired ones
      allElements = allElements.filter(element => abs(element.timestamp.getTime - currentTime.getTime) < SECONDS_OF_WINDOW * 1000)
      allElements.checkpoint()

      // 8. Remove them from trees
      val nodeCountsToDecrease = elementsToDeleteRDD.flatMap(element => {
        var map: List[((Integer, String), Integer)] = List()
        for (i <- 0 until NUM_OF_TREES) map = map :+ ((i: Integer, element.belongsToNode(i)), 1: Integer)
        map
      }).reduceByKeyLocally(_ + _).toList.groupBy(_._1._1).map(tuple => (tuple._1, tuple._2.map(t => (t._1._2, t._2))))
      forest.deleteElementsFromAllTrees(nodeCountsToDecrease)

      // 9. Break nodes where necessary
      println("Breaking nodes")
      forest.breakNodesWhereNecessary(allElements)

      // 10. Update all elements
      println("Updating outlierness scores")
      forest.updateNodesOutliernessScore()
      allElements = forest.updateElements(allElements)

      println("Done!")

      val top10 = allElements.takeOrdered(10)(Ordering[Double].reverse.on(x => x.outlierScore))
      top10.foreach(el => println("Element (" + el.vector(0) + ", " + el.vector(1) + ") - Outlierness score: " + el.outlierScore))
    })

    System.out.println("Starting stream...")
    ssc.start()
    ssc.awaitTermination()
  }

}
