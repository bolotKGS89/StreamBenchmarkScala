package Constants

object TrafficMonitoringConstants extends BaseConstants {
  val DEFAULT_PROPERTIES = "/trafficmonitoring/tm.properties"
  val DEFAULT_TOPO_NAME = "TrafficMonitoring"
  val BEIJING_SHAPEFILE = "../../Datasets/TM/beijing/roads.shp"

  object Conf {
    val RUNTIME = "tm.runtime_sec"
    val BUFFER_SIZE = "tm.buffer_size"
    val POLLING_TIME = "tm.polling_time_ms"
    val SPOUT_BEIJING = "tm.spout.beijing"
    val MAP_MATCHER_SHAPEFILE = "tm.map_matcher.shapefile"
    val MAP_MATCHER_BEIJING_MIN_LAT = "tm.map_matcher.beijing.lat.min"
    val MAP_MATCHER_BEIJING_MAX_LAT = "tm.map_matcher.beijing.lat.max"
    val MAP_MATCHER_BEIJING_MIN_LON = "tm.map_matcher.beijing.lon.min"
    val MAP_MATCHER_BEIJING_MAX_LON = "tm.map_matcher.beijing.lon.max"
    val ROAD_FEATURE_ID_KEY = "tm.road.feature.id_key"
    val ROAD_FEATURE_WIDTH_KEY = "tm.road.feature.width_key"
  }

  object Component {
    val MAP_MATCHER = "map_matcher_bolt"
    val SPEED_CALCULATOR = "speed_calculator_bolt"
  }

//  trait Component extends BaseConstants.BaseComponent {}

  object Field {
    val VEHICLE_ID = "vehicleID"
    val SPEED = "speed"
    val BEARING = "bearing"
    val LATITUDE = "latitude"
    val LONGITUDE = "longitude"
    val ROAD_ID = "roadID"
    val AVG_SPEED = "averageSpeed"
    val COUNT = "count"
  }

//  trait Field extends BaseConstants.BaseField {}

  // cities supported by the application
  object City extends Serializable {
    val BEIJING = "beijing"
  }

  // constants used to parse Beijing taxi traces
  object BeijingParsing {
    val B_VEHICLE_ID_FIELD = 0 // carID

    val B_NID_FIELD = 1
    val B_DATE_FIELD = 2
    val B_LATITUDE_FIELD = 3
    val B_LONGITUDE_FIELD = 4
    val B_SPEED_FIELD = 5
    val B_DIRECTION_FIELD = 6
  }
}
