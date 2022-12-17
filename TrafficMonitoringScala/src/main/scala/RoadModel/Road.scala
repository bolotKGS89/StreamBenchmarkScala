package RoadModel

import scala.collection.mutable

class Road(private val roadID: Int) {
  this.roadSpeed = new mutable.Queue[Integer](30)
  final private var roadSpeed: mutable.Queue[Integer] = null
  private var averageSpeed = 0
  private var count = 0

  def getRoadID: Int = roadID

  def getAverageSpeed: Int = averageSpeed

  def setAverageSpeed(averageSpeed: Int): Unit = {
    this.averageSpeed = averageSpeed
  }

  def getCount: Int = count

  def setCount(count: Int): Unit = {
    this.count = count
  }

  def incrementCount(): Unit = {
    this.count += 1
  }

  def getRoadSpeed: mutable.Queue[Integer] = roadSpeed

  def addRoadSpeed(speed: Int): Boolean = {
    roadSpeed.enqueue(speed)
    true
  }

  def getRoadSpeedSize: Int = roadSpeed.size
}