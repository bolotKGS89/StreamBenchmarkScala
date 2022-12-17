package Util

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

import java.io.File
import java.util
import java.util.{ArrayList, List}
import scala.collection.mutable.ListBuffer

class Sampler(private val samplesPerSeconds: Long) // constructor
{
  private var samples: ListBuffer[Double] = new ListBuffer[Double]
  private var epoch: Long = System.nanoTime
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
      samples += value
      counter += 1
    }
  }

  // getValues method
  def getValues: ListBuffer[Double] = samples

  // getTotal method
  def getTotal: Long = total
}
