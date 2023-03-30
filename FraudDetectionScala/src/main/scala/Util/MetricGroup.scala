package Util

import java.io.IOException
import scala.collection.mutable.Stack
import scala.collection.mutable.HashMap

object MetricGroup extends Serializable{
  private val map: HashMap[String, Stack[Sampler]] = HashMap.empty

  def add(name: String, sampler: Sampler): Unit = {
    val samplers = map.getOrElseUpdate(name, Stack[Sampler]())
    samplers.push(sampler)
  }

  // this consumes the groups
  @throws(classOf[IOException])
  def dumpAll(): Unit = {
    for ((name, _) <- map) {
      val metric = getMetric(name)
      metric.dump()
    }
  }

  // getMetric method
  private def getMetric(name: String): Metric = {
    val metric = new Metric(name)
    // consume all the groups
    map.get(name).map { samplers =>
      while (samplers.nonEmpty) {
        val sampler = samplers.pop()
        metric.setTotal(sampler.getTotal)
        // add all the values from the sampler
        sampler.getValues.map((_) => metric.add(_))
      }
    }
    metric
  }
}
