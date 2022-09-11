package object tool {
  /*

  object Alias {
    type Point = Seq[Double]
    type Center  = Seq[Double]
    type Label = Int
  }

  import Alias._

  import scala.util.Random

  class KMeans (val nbrCluster : Int) {

    def initializeMeans(points: Seq[Point]) : Seq[Center] = {
      val rand = new Random(7)
      (0 to nbrCluster).map(_ => points(rand.nextInt(points.length))).toList
    }

    def distance(point: Point, center: Point) : Double = {
      assert(point.length == center.length)
      var distance : Double = 0
      for(i <- point.indices)
        distance += scala.math.pow(point(i) - center(i), 2)
      distance
    }

    def findClosest(point: Point, centers: Seq[Center]) : Center = {
      assert(centers.nonEmpty)
      var minDistance = Double.MaxValue
      var closest : Center = null
      for(center <- centers){
        val currentDistance : Double = distance(point, center)
        if(currentDistance < minDistance){
          minDistance = currentDistance
          closest = center
        }
      }
      closest
    }

    def classify(points: Seq[Point], centers: Seq[Center]) : Map[Center, Seq[Point]] = {
      var classes : Map[Center, Seq[Point]] = centers.map( center => center -> Seq()).toMap
      points.foreach(point => {
        val center  = findClosest(point, centers)
        classes = classes + (center -> (classes.get(center) :+ point))
      })
      classes
    }

    def findAverage(oldMean: Center, points: Seq[Point]) : Center = ???
    def update(classified: Map[Center, Seq[Point]], oldMeans: Seq[Center]) : Seq[Center] = ???
    def converged(oldMeans: Seq[Center], newMeans: Seq[Center], alpha: Double) : Boolean = ???
    def fit(points: Seq[Point]) : (Seq[Center], Map[Point, Label]) = ???
  }

   */
}
