package structure

import scala.math.sqrt

// TODO: Works for 2d points only
class QuadNode(parentId: String, id: String, x0: Double, y0: Double, w: Double, h: Double) extends Serializable {

  val MAX_ELEMENTS_ALLOWED: Integer = 20

  var hasChildren: Boolean = false

  var counts: Array[Long] = new Array[Long](id.length)

  var outlierScore: Double = 0.0

  def getId: String = id
  def getLevel: Integer = id.length - 1
  def getCount: Long = counts(id.length - 1)

  def increaseCounts(countsMap: List[(Integer, Long)]): Unit = {
    for (pair <- countsMap) {
      counts(pair._1) = counts(pair._1) + pair._2
    }
  }

  def decreaseCounts(countsMap: List[(Integer, Long)]): Unit = {
    for (pair <- countsMap) {
      counts(pair._1) = counts(pair._1) - pair._2
    }
  }

  def increaseCountsOnNodeBreak(parentCounts: Array[Long], elementsCount: Long): Unit = {
    for (i <- parentCounts.indices) {
      counts(i) = parentCounts(i)
    }
    counts(counts.length - 1) = elementsCount
  }

  def distanceFromElement(element: Element): Double = {
    // TODO: Works for 2d points only
    val dim = 2
    val cVector = new Array[Double](dim)
    cVector(0) = (x0 + w)/2
    cVector(1) = (y0 + h)/2

    val centerPoint: Element = new Element(cVector, dim)
    centerPoint.distance(element)
  }

  def elementInBoundaries(el: Element): Boolean = {
    el.vector(0) >= x0 && el.vector(0) <= x0 + w && el.vector(1) >= y0 && el.vector(1) <= y0 + h
  }

  def breakToChildren(elements: List[Element]): List[QuadNode] = {

    if (needToBreak && !hasChildren){

      // Splitting elements to 4 squares, creating 4 new nodes, and pass the elements over to them
      val quadNode0 = new QuadNode(id, id + "0", x0, y0, w/2, h/2)
      quadNode0.increaseCountsOnNodeBreak(counts, elements.count(el => quadNode0.elementInBoundaries(el)))

      val quadNode1 = new QuadNode(id, id + "1", x0 + w/2, y0, w/2, h/2)
      quadNode1.increaseCountsOnNodeBreak(counts, elements.count(el => quadNode1.elementInBoundaries(el)))

      val quadNode2 = new QuadNode(id, id + "2", x0, y0 + h/2, w/2, h/2)
      quadNode2.increaseCountsOnNodeBreak(counts, elements.count(el => quadNode2.elementInBoundaries(el)))

      val quadNode3 = new QuadNode(id, id + "3", x0 + w/2, y0 + h/2, w/2, h/2)
      quadNode3.increaseCountsOnNodeBreak(counts, elements.count(el => quadNode3.elementInBoundaries(el)))

      hasChildren = true // node now has its elements in its children

      var list = List(quadNode0, quadNode1, quadNode2, quadNode3) // creating a List of nodes-children

      // Recursively break children to grandchildren if needed
      if (quadNode0.needToBreak()) list = list ::: quadNode0.breakToChildren(elements.filter(el => quadNode0.elementInBoundaries(el)))
      if (quadNode1.needToBreak()) list = list ::: quadNode1.breakToChildren(elements.filter(el => quadNode1.elementInBoundaries(el)))
      if (quadNode2.needToBreak()) list = list ::: quadNode2.breakToChildren(elements.filter(el => quadNode2.elementInBoundaries(el)))
      if (quadNode3.needToBreak()) list = list ::: quadNode3.breakToChildren(elements.filter(el => quadNode3.elementInBoundaries(el)))

      list
    } else {
      List() // or an empty list if break didn't happen
    }
  }

  private def needToBreak(): Boolean = { getCount > MAX_ELEMENTS_ALLOWED }

  def getBoundaries: String = {
    "(X[" + x0 + ", " + (x0+w) + "], Y[" + y0 + ", " + (y0+h) + "])"
  }

  def calculate_MDEF_norm(): Unit = {
    val ALPHA: Integer = 4

    var sampling = 0
    var counting = sampling + ALPHA

    var mdef = Double.MinPositiveValue

    if (counts.length == 1)
      mdef = 0
    else if (counts.length > 1 && counting <= ALPHA)
      counting = counts.length - 1

    while (counting < counts.length && sampling < counting) {
      var newMDEF: Double = Double.MinPositiveValue

      val samplingCount = counts(sampling)
      val countingCount = counts(counting)

      var s1: Long = 0
      var s2: Long = 0
      var s3: Long = 0

      for (i <- (sampling + 1) to counting) {
        s1 = s1 + counts(i)
        s2 = s2 + counts(i) * counts(i)
        s3 = s3 + counts(i) * counts(i) * counts(i)
      }

      if (s2 == samplingCount) {
        newMDEF = 0.0
      } else {
        val n_hat = s2.toDouble / s1.toDouble
        val sig_n_hat = sqrt(s3 / s1 - (s2 * s2) / (s1 * s1))
        if (sig_n_hat < Double.MinPositiveValue) {
          newMDEF = 0.0
        } else {
          val mdefScore = n_hat - countingCount
          newMDEF = mdefScore / sig_n_hat
        }
      }

      if (newMDEF > mdef)
        mdef = newMDEF

      sampling = sampling + 1
      counting = counting + 1
    }

    outlierScore = mdef
  }

}
