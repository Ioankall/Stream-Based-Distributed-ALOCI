package structure

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.util.Random

class ALOCIForest(sc: SparkContext, numOfTrees: Integer, minValues: Array[Double], maxValues: Array[Double], neighborhoodSize: Int) {

  val trees: Array[QuadTree] = new Array[QuadTree](numOfTrees)

  val shift: Array[Double] = new Array[Double](minValues.length)
  trees(0) = new QuadTree(sc, 0, minValues, maxValues, shift, neighborhoodSize)

  val random: Random.type = scala.util.Random
  for (i <- 1 until numOfTrees) {
    val svec = new Array[Double](minValues.length)
    for (j <- minValues.indices) {
      svec(j) = random.nextDouble * (maxValues(j) - minValues(j))
    }
    trees(i) = new QuadTree(sc, i, minValues, maxValues, svec, neighborhoodSize)
  }

  def addElementsToAllTrees(values: Broadcast[Map[Integer, Iterable[(String, Integer)]]]): Unit = {
    for (i <- 0 until numOfTrees)
      for (valuesForTree <- values.value.get(i))
        trees(i).addElements(valuesForTree.toMap)
  }

  def deleteElementsFromAllTrees(values: Broadcast[Map[Integer, Iterable[(String, Integer)]]]): Unit = {
    for (i <- 0 until numOfTrees) {
      for (valuesForTree <- values.value.get(i)) {
        val map = valuesForTree.toMap
        trees(i).deleteElements(map)
      }
      trees(i).trimTree()
    }
  }

  def breakNodesWhereNecessary(allElements: RDD[Element]): Unit = {
    for (i <- 0 until numOfTrees) {
      val idsOfNodesToBreak = sc.broadcast(trees(i).findNodesNeedToBreak().map(_._1).collect())
      val necessaryElements = sc.broadcast(allElements.filter(el => idsOfNodesToBreak.value.contains(el.belongsToNode(i))).collect().toList)
      trees(i).breakNodes(necessaryElements)
    }
  }

  def updateElements(elements: RDD[Element]): RDD[Element] = {
    var newElements: RDD[Element] = elements
    for (i <- 0 until numOfTrees) {
      val leafs = sc.broadcast(trees(i).nodesRDD.filter(!_._2.hasChildren).map(_._2).collect())
      newElements = newElements.map(el => {
        var wasFound = false
        var j = 0
        while (j < leafs.value.length && !wasFound) {
          if (leafs.value(j).elementInBoundaries(el)) {
            el.updateTreeInfoForElement(i, leafs.value(j).getId, leafs.value(j).distanceFromElement(el), leafs.value(j).outlierScore)
            wasFound = true
          }
          j = j + 1
        }
        el
      })
    }
    newElements
  }

  def updateNodesOutliernessScore(): Unit = {
    for (i <- 0 until numOfTrees)
      trees(i).updateOutliernessScore()
  }

  def printForestInfo(): Unit = {
    for (i <- 0 until numOfTrees)
      trees(i).printTreeInfo()
  }

  def getAvgNodesInTrees(): Double = {
    var sum: Long = 0
    for (i <- 0 until numOfTrees) {
      sum = sum + trees(i).nodesRDD.count()
    }

    (1.0 * sum)/numOfTrees
  }

  def checkpointTrees(): Unit = {
    for (i <- 0 until numOfTrees) {
      trees(i).nodesRDD.checkpoint()
    }
  }

  def fixPartitions(numOfPartitions: Integer, shuffle: Boolean): Unit = {
    for (i <- 0 until numOfTrees) {
      trees(i).nodesRDD = trees(i).nodesRDD.coalesce(numOfPartitions, shuffle = shuffle)
    }
  }


  @Deprecated
  def findClosestNodeInForest(element: Element, tLevel: Integer): (Integer, QuadNode) = {
    val matchingNodes = new Array[(Integer, QuadNode)](numOfTrees)

    for (i <- 0 until numOfTrees)
      matchingNodes(i) = (trees(i).getId, trees(i).findClosestNode(element, tLevel))

    var maxDist: Double = Double.MaxValue
    var quadNode: QuadNode = null
    var treeId: Integer = -1

    for (i <- 0 until numOfTrees) {
      val dist = matchingNodes(i)._2.distanceFromElement(element)
      if (dist < maxDist) {
        maxDist = dist
        quadNode = matchingNodes(i)._2
        treeId = matchingNodes(i)._1
      }
    }

    (treeId, quadNode)
  }

  @Deprecated
  def addElementsToAllTreesOld(values: Broadcast[Map[Integer, Iterable[(String, Integer)]]]): Unit = {
    for (i <- 0 until numOfTrees) {
      for (valuesForTree <- values.value.get(i)){
        var count = 0
        for (value <- valuesForTree) {
          count = count + 1
          trees(i).addElementsToNode(value._1, value._2)
          if (count == 100) {
            count = 0
            trees(i).nodesRDD.first()
          }
        }
      }
    }
  }

  @Deprecated
  def deleteElementsFromAllTreesOld(values: Broadcast[Map[Integer, Iterable[(String, Integer)]]]): Unit = {
    for (i <- 0 until numOfTrees) {
      for (valuesForTree <- values.value.get(i)){
        var count = 0
        for (value <- valuesForTree) {
          count = count + 1
          trees(i).deleteElementsFromNode(value._1, value._2)
          if (count == 100) {
            count = 0
            trees(i).nodesRDD.first()
          }
        }
      }
    }
  }

}
