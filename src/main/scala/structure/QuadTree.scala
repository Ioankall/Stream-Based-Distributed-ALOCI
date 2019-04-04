package structure

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class QuadTree (sc: SparkContext, treeId: Integer, minValues: Array[Double], maxValues: Array[Double], shift: Array[Double], neighborhoodSize: Int) extends Serializable {

  val root: QuadNode = new QuadNode("", "0", minValues(0), minValues(1), Math.abs(maxValues(0) - minValues(0)), Math.abs(maxValues(1) - minValues(1)), neighborhoodSize)

  var nodesRDD: RDD[(String, QuadNode)] = sc.parallelize(List(root).map(n => (n.getId, n)))

  def getId: Integer = treeId

  def addElements(counts: Map[String, Integer]): Unit = {
    nodesRDD = nodesRDD.map(node => {
      var map: List[(Integer, Long)] = List()
      val nodeId = node._1
      var len = nodeId.length
      while (len > 0) {
        val id = nodeId.substring(0, len)
        val count: Long = counts.getOrElse[Integer](id, -1).toLong
        if (count > 0) map = map :+ (id.length - 1: Integer, count)
        len = len - 1
      }
      val newNode = node
      newNode._2.increaseCounts(map)
      newNode
    })
  }

  def deleteElements(counts: Map[String, Integer]): Unit = {
    nodesRDD = nodesRDD.map(node => {

      var countUpdates: List[(Integer, Long)] = List()

      val curNodeId = node._1
      var len = curNodeId.length
      while (len > 0) {
        val id = curNodeId.substring(0, len)
        val count: Long = counts.getOrElse[Integer](id, -1).toLong
        if (count > 0) countUpdates = countUpdates :+ (id.length - 1: Integer, count)
        len = len - 1
      }
      val newNode = node
      newNode._2.decreaseCounts(countUpdates)
      newNode
    })
  }

  def trimTree(): Unit = {
    val idsOfNodesToBeDeleted = nodesRDD
      .filter(node => node._2.getCount <= node._2.getNeighborhoodSize && node._2.hasChildren)
      .map(node => node._1)
      .collect()

    // Delete their children, mark them as children-less
    nodesRDD = nodesRDD.filter(node => {
      var toBeKept: Boolean = true
      for (id <- idsOfNodesToBeDeleted)
        if (node._1.startsWith(id) && node._1.length > id.length)
          toBeKept = false
      toBeKept || node._1 == "0"
    }).map(node => {
      if (idsOfNodesToBeDeleted.contains(node._1)) {
        val newNode = node
        newNode._2.hasChildren = false
        newNode
      } else {
        node
      }

    })
  }

  def findNodesNeedToBreak(): RDD[(String, QuadNode)] = nodesRDD.filter(node => node._2.getCount > node._2.getNeighborhoodSize && !node._2.hasChildren)


  def breakNodes(allElements: Broadcast[List[Element]]): Unit = {
    val nodesToBreakRDD = findNodesNeedToBreak()

    val newNodesToAddRDD = nodesToBreakRDD
      .flatMap(node => node._2.breakToChildren(allElements.value.map(el => shiftElement(el)).filter(el => node._2.elementInBoundaries(el))))
      .map(node => (node.getId, node))

    val idsOfNodesToBreak = nodesToBreakRDD.map(_._1).collect()

    nodesRDD = nodesRDD.map(node => {
      if (idsOfNodesToBreak.contains(node._1)) {
        val newNode: QuadNode = node._2
        newNode.hasChildren = true
        (newNode.getId, newNode)
      } else {
        node
      }
    })

    nodesRDD = nodesRDD.union(newNodesToAddRDD)
  }

  private def shiftElement(el: Element): Element = {
    val newVector = new Array[Double](el.vector.length)
    for (i <- el.vector.indices) {
      if (el.vector(i) < maxValues(i) - shift(i))
        newVector(i) = el.vector(i) + shift(i)
      else
        newVector(i) = minValues(i) + (shift(i) - (maxValues(i) - el.vector(i)))
    }
    new Element(newVector, el.numOfTrees)
  }

  def updateOutliernessScore(): Unit = {
//    nodesRDD.foreach(n => println(n._1 + " -- " + n._2.getCount + " -- " + n._2.hasChildren))

    nodesRDD = nodesRDD.map(node => {
      if (!node._2.hasChildren) {
        val newNode = node
        newNode._2.calculate_MDEF_norm()
        newNode
      } else {
        node
      }
    })
  }

  def printTreeInfo(): Unit = {
    println("Tree id: " + treeId)
    println("Number of nodes: " + nodesRDD.count)
    println("Number of nodes with children: " + nodesRDD.filter(_._2.hasChildren).count)

    nodesRDD.foreach(n => {
      var str = " Parent counts: ["
      for (i <- 0 until n._1.length)
        str = str + n._2.counts(i) + ", "
      str = str + "] "
      println("Node: " + n._1 + " Level: " + n._2.getLevel + " Count: " + n._2.getCount + str + " Has children: " + n._2.hasChildren + " Boundaries: " + n._2.getBoundaries)
    })
    println("END OF TREE")
  }

  @Deprecated
  def findClosestNode(element: Element, tLevel: Integer): QuadNode = {
    var node: QuadNode = null
    var numOfNodesAtLevel: Long = 1
    (0 to tLevel).iterator.takeWhile(_ => numOfNodesAtLevel > 0).foreach(level => {
      val matchingNodesRDD = nodesRDD.filter(nodeTuple => nodeTuple._2.getLevel == level && nodeTuple._2.elementInBoundaries(shiftElement(element))).map(_._2)
      numOfNodesAtLevel = matchingNodesRDD.count
      if (matchingNodesRDD.count() == 1) node = matchingNodesRDD.first
    })
    node
  }

  @Deprecated
  def addElementsToNode(nodeId: String, count: Integer): Unit = {
    nodesRDD = nodesRDD.map(node => {
      var map: List[(Integer, Long)] = List()
      var i: Integer = 0
      var stop = false
      while (i < nodeId.length && i < node._1.length && !stop) {
        if (nodeId(i) == node._1(i)) {
          map = map :+ (i, count.toLong)
        } else {
          stop = true
        }
        i = i + 1
      }

      val newNode = node
      newNode._2.increaseCounts(map)
      newNode
    })
  }

  @Deprecated
  def deleteElementsFromNode(nodeId: String, count: Integer): Unit = {
    nodesRDD = nodesRDD.map(node => {
      var map: List[(Integer, Long)] = List()
      var i: Integer = 0
      var stop = false
      while (i < nodeId.length && i < node._1.length && !stop) {
        if (nodeId(i) == node._1(i)) {
          map = map :+ (i, count.toLong)
        } else {
          stop = true
        }
        i = i + 1
      }

      val newNode = node
      newNode._2.decreaseCounts(map)
      newNode
    })
  }

}
