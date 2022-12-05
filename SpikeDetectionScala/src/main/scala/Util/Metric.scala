package Util

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

import java.io.File

class Metric {
  private var name: String = null
  private var fileName: String = null
  private var descriptiveStatistics: DescriptiveStatistics = null
  private var total = 0L

  // constructor
  def this(name: String) {
    this()
    this.name = name
    fileName = String.format("metric_%s.json", name)
    descriptiveStatistics = new DescriptiveStatistics
  }

  // add method
  def add(value: Double): Unit = {
    descriptiveStatistics.addValue(value)
  }

  // setTotal method
  def setTotal(total: Long): Unit = {
    this.total = total
  }

  // dump method
//  @throws[IOException]
  def dump(): Unit = {
//    val objectNode = JsonNodeFactory.instance.objectNode
//    objectNode.put("name", name)
//    objectNode.put("samples", descriptiveStatistics.getN)
//    objectNode.put("total", total)
//    objectNode.put("mean", descriptiveStatistics.getMean)
//    // add percentiles
//    objectNode.put("5", descriptiveStatistics.getPercentile(5))
//    objectNode.put("25", descriptiveStatistics.getPercentile(25))
//    objectNode.put("50", descriptiveStatistics.getPercentile(50))
//    objectNode.put("75", descriptiveStatistics.getPercentile(75))
//    objectNode.put("95", descriptiveStatistics.getPercentile(95))
//    // write the JSON object to file
//    val objectMapper = new ObjectMapper
//    val objectWriter = objectMapper.writer(new DefaultPrettyPrinter)
//    objectWriter.writeValue(new File(fileName), objectNode)
  }
}
