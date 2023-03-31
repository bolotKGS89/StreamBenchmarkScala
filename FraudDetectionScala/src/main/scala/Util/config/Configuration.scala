package Util.config

import FraudDetection.SparkFraudDetection.getClass

import java.util.Properties
import scala.io.Source

class Configuration {
  val METRICS_ENABLED = "metrics.enabled"
  val METRICS_REPORTER = "metrics.reporter"
  val METRICS_INTERVAL_VALUE = "metrics.interval.value"
  val METRICS_INTERVAL_UNIT = "metrics.interval.unit"
  val METRICS_OUTPUT = "metrics.output"

  private def getPropValue(key: String): String = {
    val props = new Properties()
    val resourceStream = getClass.getResourceAsStream("/fd.properties")
    props.load(resourceStream)

    props.getProperty(key)
  }

  def getString(key: String): String = {
    var str: String = null
    val obj = getPropValue(key)

    if (null != obj) {
      if (obj.isInstanceOf[String]) {
        str = obj.asInstanceOf[String]
      } else throw new IllegalArgumentException("String value not found in configuration for " + key)
    }
    else throw new IllegalArgumentException("Nothing found in configuration for " + key)
    str
  }

  def getString(key: String, definition: String): String = {
    var str: String = null
    try  {
      str = getString(key)
    }
    catch {
      case ex: IllegalArgumentException => {
        str = definition
      }
    }
    str
  }

  def getInt(key: String): Int = {
    var str = 0
    val obj = getPropValue(key)
    if (null != obj) if (obj.isInstanceOf[Integer]) str = obj.asInstanceOf[Integer]
    else if (obj.isInstanceOf[Number]) str = obj.asInstanceOf[Number].intValue
    else if (obj.isInstanceOf[String]) {
      try {
        str = obj.asInstanceOf[String].toInt
      }
      catch {
        case ex: NumberFormatException =>
          throw new IllegalArgumentException("Value for configuration key " + key + " cannot be parsed to an Integer", ex)
      }
    }
    else throw new IllegalArgumentException("Integer value not found in configuration for " + key)
    else throw new IllegalArgumentException("Nothing found in configuration for " + key)
    str
  }

  def getLong(key: String): Long = {
    var ans:Long = 0
    val obj = getPropValue(key)
    if (null != obj) if (obj.isInstanceOf[Long]) ans = obj.asInstanceOf[Long]
    else if (obj.isInstanceOf[String]) try ans = obj.asInstanceOf[String].toLong
    catch {
      case ex: NumberFormatException =>
        throw new IllegalArgumentException("String value not found in configuration for " + key)
    }
    else throw new IllegalArgumentException("String value not found  in configuration for " + key)
    else throw new IllegalArgumentException("Nothing found in configuration for " + key)
    ans
  }

  def getInt(key: String, definition: Int): Int = {
    var ans = 0
    try ans = getInt(key)
    catch {
      case ex: Exception =>
        ans = definition
    }
    ans
  }

  def getDouble(key: String): Double = {
    val obj = getPropValue(key)
    if(obj != null) {
      if (obj.isInstanceOf[Double]) {
        return obj.toDouble
      } else if (obj.isInstanceOf[String]) {
        try {
          return obj.trim.toDouble
        }
        catch {
          case ex: NumberFormatException =>
            throw new IllegalArgumentException("String value not found in configuration for " + key)
        }
      }
      throw new IllegalArgumentException("String value not found in configuration for " + key)
    } else {
      throw new IllegalArgumentException("Nothing found in configuration for " + key)
    }
  }

  def getDouble(key: String, defined: Double): Double = {
    var ans: Double = 0
    try ans = getDouble(key)
    catch {
      case ex: Exception =>
        ans = defined
    }
    ans
  }

  def getBoolean(key: String): Boolean = {
    var bool = false
    val obj = getPropValue(key)
    if (null != obj) if (obj.isInstanceOf[Boolean]) bool = obj.asInstanceOf[Boolean]
    else if (obj.isInstanceOf[String]) bool = obj.asInstanceOf[String].toBoolean
    else throw new IllegalArgumentException("Boolean value not found  in configuration  for " + key)
    else throw new IllegalArgumentException("Nothing found in configuration for " + key)
    bool
  }

  def getBoolean(key: String, isDef: Boolean): Boolean = {
    var ans = false
    try ans = getBoolean(key)
    catch {
      case ex: Exception =>
        ans = isDef
    }
    ans
  }

  def getIntArray(key: String, separator: String): Array[Int] = {
    val value = getString(key)
    val items = value.split(separator)
    val values = new Array[Int](items.length)
    for (i <- 0 until items.length) {
      try values(i) = items(i).toInt
      catch {
        case ex: NumberFormatException =>
          throw new IllegalArgumentException("Value for configuration key " + key + " cannot be parsed to an Integer array", ex)
      }
    }
    values
  }

  def getIntArray(key: String, separator: String, defined: Array[Int]): Array[Int] = {
    var values: Array[Int] = null
    try values = getIntArray(key, separator)
    catch {
      case ex: IllegalArgumentException =>
        values = defined
    }
    values
  }

  def getIntArray(key: String, defined: Array[Int]): Array[Int] = getIntArray(key, ",", defined)

  def strToMap(str: String): scala.collection.Map[String, String] = {
    val map: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty[String, String]
    val arguments: Array[String] = str.split(",")
    for (arg <- arguments) {
      val kv: Array[String] = arg.split("=")
      map += (kv(0).trim -> kv(1).trim)
    }
    map
  }



}
