package RoadModel

import Constants.TrafficMonitoringConstants.Conf
import Util.config.Configuration
import org.apache.spark.internal.config

import java.io.File
import java.nio.charset.Charset
import java.util
import java.util.{ArrayList, HashMap}

class RoadGridList(config: Configuration, path: String) {
  private var gridList = new util.HashMap[String, util.ArrayList[Nothing]]
  private var idKey: String = null
  private var widthKey: String = null
  gridList = read(path)

  idKey = config.getString(Conf.ROAD_FEATURE_ID_KEY) // attribute of the SimpleFeature containing the ID of the road

  widthKey = config.getString(Conf.ROAD_FEATURE_WIDTH_KEY, null) // width of the cells of the grid into which the map is divided




}
