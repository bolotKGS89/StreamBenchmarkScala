package RoadModel

class Point(val x:Double, val y:Double) {

  def getX: Double = x

  def getY: Double = y
}

class GPSRecord(x: Double, y: Double, speed: Double = .0, direction: Int = 0)  {


  def getSpeed: Double = speed

  def getDirection: Int = direction
}
