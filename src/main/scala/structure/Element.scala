package structure

import java.sql.Timestamp
import java.time.LocalDateTime

class Element (var vector: Array[Double], var numOfTrees: Integer) extends Serializable {

  var timestamp: Timestamp = Timestamp.valueOf(LocalDateTime.now())

  var belongsToNode: Array[String] = new Array[String](numOfTrees)
  var distanceFromNode: Array[Double] = new Array[Double](numOfTrees)
  var outlierScores: Array[Double] = new Array[Double](numOfTrees)

  for (i <- 0 until numOfTrees) belongsToNode(i) = "-1"

  def updateTimeStamp: Unit = {
    timestamp = Timestamp.valueOf(LocalDateTime.now())
  }

  def outlierScore: Double = {
    var minDistance: Double = Double.MaxValue
    var score: Double = 0.0
    for (i <- distanceFromNode.indices) {
      if (distanceFromNode(i) < minDistance) {
        minDistance = distanceFromNode(i)
        score = outlierScores(i)
      }
    }
    score
  }

  def distance(element: Element) : Double = {
    var squareDifSum: Double = 0
    for (i <- 0 until vector.size)
      squareDifSum = squareDifSum + Math.pow(vector(i) - element.vector(i), 2)
    Math.sqrt(squareDifSum)
  }

  def updateTreeInfoForElement(treeId: Integer, nodeId: String, distance: Double, outlierScore: Double) : Unit = {
    belongsToNode(treeId) = nodeId
    distanceFromNode(treeId) = distance
    outlierScores(treeId) = outlierScore
  }

}
