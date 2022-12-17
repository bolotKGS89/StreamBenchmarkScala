package Util

import scala.collection.mutable
import java.util.{HashMap, Set, Stack}
import java.io.IOException
import Util.Sampler



class MetricGroup {
  private val map = mutable.Map.empty[String, Stack[Sampler]]

  // this is not time critical, making the whole method synchronized is good enough
  def add(name: String, sampler: Sampler): Unit = {
    val samplers: Stack[Sampler] = map.getOrElseUpdate(name, new Stack[Sampler])
    samplers.add(sampler)
  }

  // this consumes the groups
  @throws[IOException]
  def dumpAll(): Unit = {
    map.keySet.map((key) => {
      val metric: Metric = getMetric(key)
      metric.dump()
    })
  }

  // getMetric method
  private def getMetric(name: String): Metric = {
    val metric: Metric = new Metric(name)
    // consume all the groups
    val samplers: Stack[Sampler] = map(name)
    while (!(samplers.empty)) {
      val sampler: Sampler = samplers.pop
      metric.setTotal(sampler.getTotal)
      // add all the values from the sampler
      for (value <- sampler.getValues) {
        metric.add(value)
      }
    }
    metric
  }
}
