package structure

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random

class ALOCIForest(sc: SparkContext, numOfTrees: Integer, minValues: Array[Double], maxValues: Array[Double]) {

  val trees: Array[QuadTree] = new Array[QuadTree](numOfTrees)

  val shift: Array[Double] = new Array[Double](minValues.length)
  trees(0) = new QuadTree(sc, 0, minValues, maxValues, shift)

  val random: Random.type = scala.util.Random
  for (i <- 1 until numOfTrees) {
    val svec = new Array[Double](minValues.length)
    for (j <- minValues.indices) {
      svec(j) = random.nextDouble * (maxValues(j) - minValues(j))
    }
    trees(i) = new QuadTree(sc, i, minValues, maxValues, svec)
  }

  def addElementsToAllTrees(values: Map[Integer, Iterable[(String, Integer)]]): Unit = {
    for (i <- 0 until numOfTrees)
      for (valuesForTree <- values.get(i))
        trees(i).addElements(valuesForTree.toMap)
  }

  def deleteElementsFromAllTrees(values: Map[Integer, Iterable[(String, Integer)]]): Unit = {
    for (i <- 0 until numOfTrees) {
      for (valuesForTree <- values.get(i))
        trees(i).deleteElements(valuesForTree.toMap)
      trees(i).trimTree()
    }
  }

  def breakNodesWhereNecessary(allElements: RDD[Element]): Unit = {
    for (i <- 0 until numOfTrees) {
      val idsOfNodesToBreak = trees(i).findNodesNeedToBreak().map(_._1).collect()
      val necessaryElements = allElements.filter(el => idsOfNodesToBreak.contains(el.belongsToNode(i))).collect().toList
      trees(i).breakNodes(necessaryElements)
    }
  }

  def updateElements(elements: RDD[Element]): RDD[Element] = {
    var newElements: RDD[Element] = elements
    for (i <- 0 until numOfTrees) {
      val leafs = trees(i).nodesRDD.filter(!_._2.hasChildren).map(_._2).collect()
      newElements = newElements.map(el => {
        var wasFound = false
        var j = 0
        while (j < leafs.length && !wasFound) {
          if (leafs(j).elementInBoundaries(el)) {
            el.updateTreeInfoForElement(i, leafs(j).getId, leafs(j).distanceFromElement(el), leafs(j).outlierScore)
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

}
