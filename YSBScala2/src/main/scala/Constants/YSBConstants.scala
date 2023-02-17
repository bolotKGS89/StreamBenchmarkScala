package Constants

object YSBConstants {
  val DEFAULT_PROPERTIES = "/YSB/YSB.properties"
  val DEFAULT_TOPO_NAME = "YSB"

  object Conf {
    val RUNTIME = "yb.runtime_sec"
    val BUFFER_SIZE = "yb.buffer_size"
    val POLLING_TIME = "yb.polling_time_ms"
    val NUM_KEYS = "yb.numKeys"
  }

  object Component {
    val FILTER = "filter"
    val JOINER = "joiner"
    val WINAGG = "winAggregate"
  }

  object Field {
    val UUID = "uuid"
    val UUID2 = "uuid2"
    val AD_ID = "ad_id"
    val AD_TYPE = "ad_type"
    val EVENT_TYPE = "event_type"
    val TIMESTAMP = "timestamp"
    val IP = "ip"
    val CMP_ID = "cmp_id"
    val COUNTER = "counter"
  }

}
