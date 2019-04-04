package structure

class Element (val vector: Array[Double], val numOfTrees: Integer) extends Serializable {

  var timestamp: Long = System.currentTimeMillis()

  var belongsToNode: Array[String] = new Array[String](numOfTrees)
  var distanceFromNode: Array[Double] = new Array[Double](numOfTrees)
  var outlierScores: Array[Double] = new Array[Double](numOfTrees)

  @Deprecated
  var outlierScoreOld: Double = 0

  for (i <- 0 until numOfTrees) belongsToNode(i) = "-1"

  def updateTimeStamp(time: Long): Unit = {
    timestamp = time
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
