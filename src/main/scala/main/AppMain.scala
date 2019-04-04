package main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import structure.{ALOCIForest, Element, QuadNode, QuadTree}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream

import scala.math.abs
import scala.collection.mutable


object AppMain {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Default values
  val COUNT_BASED_WINDOW_VALUE = "count-based"
  val LOCAL_MODE_VALUE = "local"
  val CLUSTER_MODE_VALUE = "cluster"
  val WINDOW_SIZE_DEFAULT_VALUE = "10000000"
  val NUM_OF_TREES_DEFAULT_VALUE = "1"
  val ALPHA_DEFAULT_VALUE = "4"
  val PARTITIONS_DEFAULT_VALUE = "128"
  val N_SIZE_DEFAULT_VALUE = "40"
  val BATCHES_DEFAULT_VALUE = "12"

  val LOCAL_INPUT_PATH = "./resources/cities/"
  val CLUSTER_INPUT_PATH = "hdfs://master2-bigdata/user/sparklab/aloci/files/"
  val LOCAL_TEMP_DIR = "./tmp"
  val CLUSTER_TEMP_DIR = "hdfs://master2-bigdata/user/sparklab/aloci/tmp"


  // Configuration
  val BATCH_INTERVAL_SECONDS = 30
  val USE_OPTIMIZATION_ON_OUTLIERNESS_SCORE_CALCULATION: Boolean = true
  val USE_OPTIMIZATION_ON_INSERT_AND_DELETE: Boolean = true

  def main(args: Array[String]): Unit = {

    val argList = args.toList
    type OptionMap = Map[Symbol, String]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {

      def isSwitch(s : String) = s(0) == '-'
      list match {
        case Nil => map

        case "--window-type" :: value :: tail =>
          if (value.equals("count-based") || value.equals("time-based")) {
            nextOption(map ++ Map('windowType -> value), tail)
          } else {
            println("Unknown value for --window-type: " + value)
            sys.exit(1)
          }

        case "--window-size" :: value :: tail =>
          try {
            value.toInt
            nextOption(map ++ Map('windowSize -> value), tail)
          } catch {
            case e: Exception => {
              println("Value for --window-size is not an integer: " + value)
              sys.exit(1)
            }
          }

        case "--trees" :: value :: tail =>
          try {
            value.toInt
            nextOption(map ++ Map('numOfTress -> value), tail)
          } catch {
            case e: Exception => {
              println("Value for --trees is not an integer: " + value)
              sys.exit(1)
            }
          }

        case "--alpha" :: value :: tail =>
          try {
            value.toInt
            nextOption(map ++ Map('alpha -> value), tail)
          } catch {
            case e: Exception => {
              println("Value for --alpha is not an integer: " + value)
              sys.exit(1)
            }
          }

        case "--neighborhood-size" :: value :: tail =>
          try {
            value.toInt
            nextOption(map ++ Map('neighborhoodSize -> value), tail)
          } catch {
            case e: Exception => {
              println("Value for --neighborhood-size is not an integer: " + value)
              sys.exit(1)
            }
          }

        case "--mode" :: value :: tail =>
          if (value.equals(LOCAL_MODE_VALUE) || value.equals(CLUSTER_MODE_VALUE)) {
            nextOption(map ++ Map('mode -> value), tail)
          } else {
            println("Unknown value for --mode: " + value)
            sys.exit(1)
          }

        case "--partitions" :: value :: tail =>
          try {
            value.toInt
            nextOption(map ++ Map('partitions -> value), tail)
          } catch {
            case e: Exception => {
              println("Value for --partitions is not an integer: " + value)
              sys.exit(1)
            }
          }

        case "--batches" :: value :: tail =>
          try {
            value.toInt
            nextOption(map ++ Map('batches -> value), tail)
          } catch {
            case e: Exception => {
              println("Value for --batches is not an integer: " + value)
              sys.exit(1)
            }
          }

        case option :: _ => println("Unknown option "+option)
          sys.exit(1)
      }
    }

    val options = nextOption(Map(), argList)
    println(options)

    val FILTERING_OUT_METHOD: String = options.getOrElse[String]('windowType, COUNT_BASED_WINDOW_VALUE)
    val WINDOW_SIZE: Integer = options.getOrElse[String]('windowSize, WINDOW_SIZE_DEFAULT_VALUE).toInt
    val NUM_OF_TREES: Integer = options.getOrElse[String]('numOfTress, NUM_OF_TREES_DEFAULT_VALUE).toInt
    val ALPHA: Integer = options.getOrElse[String]('alpha, ALPHA_DEFAULT_VALUE).toInt
    val LOCAL_MODE: Boolean = options.getOrElse[String]('mode, CLUSTER_MODE_VALUE).equals(LOCAL_MODE_VALUE)
    val NUMBER_OF_PARTITIONS: Integer = options.getOrElse[String]('partitions, PARTITIONS_DEFAULT_VALUE).toInt
    val NEIGHBORHOOD_SIZE: Integer = options.getOrElse[String]('neighborhoodSize, N_SIZE_DEFAULT_VALUE).toInt
    val NUMBER_OF_BATCHES: Integer = options.getOrElse[String]('batches, BATCHES_DEFAULT_VALUE).toInt

    var tempDir: String = ""
    var inputFilesPath = ""

    val sparkConf = new SparkConf().setAppName("ALOCI")

    if (LOCAL_MODE) {
      sparkConf.setMaster("local[*]")
      inputFilesPath = LOCAL_INPUT_PATH
      tempDir = LOCAL_TEMP_DIR
    } else {
      inputFilesPath = CLUSTER_INPUT_PATH
      tempDir = CLUSTER_TEMP_DIR
    }

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(BATCH_INTERVAL_SECONDS))
    val inputData: mutable.Queue[RDD[Element]] = mutable.Queue()
    val inputStream: InputDStream[Element] = ssc.queueStream(inputData)

    ssc.sparkContext.setCheckpointDir(tempDir: String)

    for (i <- 0 until NUMBER_OF_BATCHES) {
      var inputRDD = ssc.sparkContext.textFile(inputFilesPath + "batch_" + i + ".txt", NUMBER_OF_PARTITIONS)
        .map(line => line.split(','))
        .map(line => Array(line(0).toDouble, line(1).toDouble))
        .map(vector => new Element(vector, NUM_OF_TREES))

      val duplicates = inputRDD.map(el => ((el.vector(0), el.vector(1)), 1)).reduceByKey(_+_).filter(t => t._2 > 1).map(_._1).collect().toList

      val duplToAdd = sc.parallelize(duplicates.map(el => new Element(Array(el._1.toDouble, el._2.toDouble), NUM_OF_TREES)))
      inputRDD = inputRDD.filter(el => !duplicates.contains((el.vector(0), el.vector(1))))
      inputRDD = inputRDD.union(duplToAdd)
      inputData.enqueue(inputRDD)
    }

    var allElements: RDD[Element] = ssc.sparkContext.emptyRDD[Element]

    val forest: ALOCIForest = new ALOCIForest(ssc.sparkContext, NUM_OF_TREES, Array(-90,-180), Array(90,180), NEIGHBORHOOD_SIZE)

    var batchCount = 0
    inputStream.foreachRDD(rdd => {
      val t0 = System.currentTimeMillis()

      // 1. Get new batch of elements in RDD[Element]
      val insertTime = System.currentTimeMillis()
      val currentTimeRDD = rdd.map(element => {
        val newElement = element
        newElement.updateTimeStamp(insertTime)
        newElement
      })

      // 2. Update them with their assigned nodes
      val updatedRDD = forest.updateElements(currentTimeRDD)

      // 3. Compute necessary increasing count map
      val nodeCountsToIncr = sc.broadcast(updatedRDD.flatMap(element => {
        var map: List[((Integer, String), Integer)] = List()
        for (i <- 0 until NUM_OF_TREES)
          for (j <- 1 to element.belongsToNode(i).length)
            map = map :+ ((i: Integer, element.belongsToNode(i).substring(0, j)), 1: Integer)
        map
      }).reduceByKey(_ + _)
        .groupBy(_._1._1)
        .map(tuple => (tuple._1, tuple._2.map(t => (t._1._2, t._2))))
        .collectAsMap())

      // 4. Add counts to nodes
      if (USE_OPTIMIZATION_ON_INSERT_AND_DELETE) {
        forest.addElementsToAllTrees(nodeCountsToIncr)
      } else {
        forest.addElementsToAllTreesOld(nodeCountsToIncr)
      }

      // 5. Union new elements to allElements
      allElements = allElements.union(updatedRDD)
      allElements.checkpoint()

      // 6. Find expiring elements and filter them out
      var elementsToDeleteRDD: RDD[Element] = sc.emptyRDD
      var deleting: Long = 0

      if (FILTERING_OUT_METHOD.equals(COUNT_BASED_WINDOW_VALUE)) {
        val indexedElements20: RDD[(Element, Long)] = allElements.sortBy(element => element.timestamp, ascending = false).zipWithIndex()
        if (indexedElements20.count() > WINDOW_SIZE) {
          elementsToDeleteRDD = indexedElements20.filter(_._2 >= WINDOW_SIZE).map(_._1)
          deleting = elementsToDeleteRDD.count()
          allElements = indexedElements20.filter(_._2 < WINDOW_SIZE).map(_._1)
          allElements.checkpoint()
        }
      } else {
        val currentTime = System.currentTimeMillis()
        elementsToDeleteRDD = allElements.filter(element => abs(element.timestamp - currentTime) >= WINDOW_SIZE * 1000)
        deleting = elementsToDeleteRDD.count()
        allElements = allElements.filter(element => abs(element.timestamp - currentTime) < WINDOW_SIZE * 1000)
        allElements.checkpoint()
      }


      // 7. Remove them from trees
      if (deleting > 0) {
        val nodeCountsToDecrease = sc.broadcast(elementsToDeleteRDD.flatMap(element => {
          var map: List[((Integer, String), Integer)] = List()
          for (i <- 0 until NUM_OF_TREES)
            for (j <- 1 to element.belongsToNode(i).length)
              map = map :+ ((i: Integer, element.belongsToNode(i).substring(0, j)), 1: Integer)
          map
        }).reduceByKey(_ + _)
          .groupBy(_._1._1)
          .map(tuple => (tuple._1, tuple._2.map(t => (t._1._2, t._2))))
          .collectAsMap())

        if (USE_OPTIMIZATION_ON_INSERT_AND_DELETE) {
          forest.deleteElementsFromAllTrees(nodeCountsToDecrease)
        } else {
          forest.deleteElementsFromAllTreesOld(nodeCountsToDecrease)
        }
      }

      // 8. Break nodes where necessary
      forest.breakNodesWhereNecessary(allElements)

      // 9. Update all elements
      if (USE_OPTIMIZATION_ON_OUTLIERNESS_SCORE_CALCULATION) {
        forest.updateNodesOutliernessScore()
        forest.checkpointTrees()
        allElements = forest.updateElements(allElements)

        forest.updateNodesOutliernessScore()
        forest.checkpointTrees()
        allElements = forest.updateElements(allElements)
      } else {
        val collectedElements = allElements.collect.map(element => {
          // Find appropriate tree for element
          var tree: QuadTree = null

          var maxmdefnorm: Double = 0
          var ci: String = null
          var cj: String = null

          var level = ALPHA //for level < alpha cj is null
          var toContinueForever: Boolean = true
          while (toContinueForever) {
            val result = forest.findClosestNodeInForest(element, level)
            tree = forest.trees(result._1)
            val ciNode: QuadNode = result._2
            if (ciNode.getLevel != level) {
              toContinueForever = false
            }

            var cjNode: QuadNode = null
            if (ciNode == null) {
              toContinueForever = false
            } else {
              ci = ciNode.getId
              cjNode = tree.findClosestNode(element, level - ALPHA)
            }

            if (cjNode != null) {
              cj = cjNode.getId
              val mdefnorm: Double = calculate_MDEF_norm(tree, cjNode, ciNode)
              if (mdefnorm > maxmdefnorm) maxmdefnorm = mdefnorm
            }

            level = level + 1
          }

          val newElement: Element = element
          newElement.outlierScoreOld = maxmdefnorm
          newElement

        })
        allElements = sc.parallelize(collectedElements)
      }

      // 10. Fix partitioning issue with very large number of partitions after multiple unions
      forest.fixPartitions(NUMBER_OF_PARTITIONS, shuffle = true)
      allElements = allElements.repartition(NUMBER_OF_PARTITIONS)

      val t1 = System.currentTimeMillis()
      println(batchCount + ", " + allElements.count() + ", " + forest.getAvgNodesInTrees()+ ", " + (t1-t0)/1000 )


      if (batchCount >= NUMBER_OF_BATCHES) {
        ssc.stop(stopSparkContext = true)
        sc.stop()
      } else {
        batchCount = batchCount + 1
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  @Deprecated
  def calculate_MDEF_norm(tree: QuadTree, samplingNode: QuadNode, countingNode: QuadNode): Double = {
    // usually sampling and counting nodes are alpha levels apart? Is it always? :/
    val nodes = tree.nodesRDD
      .filter(nodeTuple => nodeTuple._1.startsWith(samplingNode.getId) && // level >= cj's level
        nodeTuple._2.getLevel <= countingNode.getLevel) // level <= ci's level
      .collect()

    val samplingCount = samplingNode.getCount
    val countingCount = countingNode.getCount

    val sq: Long = nodes
      .filter(nodeTuple => nodes.count(n => n._1.startsWith(nodeTuple._1)) == 1)
      .map(t => t._2.getCount).map(c => c * c)
      .sum

    if (sq == samplingCount) return 0.0


    val cb: Long = nodes
      .filter(nodeTuple => nodes.count(n => n._1.startsWith(nodeTuple._1)) == 1)
      .map(t => t._2.getCount).map(c => c * c * c)
      .sum

    val n_hat = sq.toDouble / samplingCount

    val sig_n_hat = Math.sqrt(cb * samplingCount - (sq * sq)) / samplingCount

    if (sig_n_hat < Double.MinPositiveValue) return 0.0

    val mdef = n_hat - countingCount

    mdef / sig_n_hat
  }

}
