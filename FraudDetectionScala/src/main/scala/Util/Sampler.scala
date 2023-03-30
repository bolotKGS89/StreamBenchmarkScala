package Util


import scala.collection.mutable

class Sampler(val samplesPerSeconds: Long) extends Serializable// constructor
{
  epoch = System.nanoTime
  counter = 0
  total = 0
  private var samples: mutable.ListBuffer[Double] = new mutable.ListBuffer[Double]
  private var epoch: Long = 0L
  private var counter: Long = 0L
  private var total: Long = 0L


  // add method
  def add(value: Double): Unit = {
    add(value, 0)
  }

  // add method
  def add(value: Double, timestamp: Long): Unit = {
    total += 1
    // add samples according to the sample rate
    val seconds: Double = (timestamp - epoch) / 1e9
    if (samplesPerSeconds == 0 || counter <= samplesPerSeconds * seconds) {
      samples.+=(value)
      counter += 1
    }
  }

  // getValues method
  def getValues: List[Double] = samples.toList

  // getTotal method
  def getTotal: Long = total
}