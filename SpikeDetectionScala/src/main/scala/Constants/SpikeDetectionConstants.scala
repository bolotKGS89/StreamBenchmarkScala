package Constants

object SpikeDetectionConstants {
  val DEFAULT_PROPERTIES = "/spikedetection/sd.properties"
  val DEFAULT_TOPO_NAME = "SpikeDetection"
  val DEFAULT_THRESHOLD = 0.03d

  object Conf {
    val RUNTIME = "sd.runtime_sec"
    val BUFFER_SIZE = "sd.buffer_size"
    val POLLING_TIME = "sd.polling_time_ms"
    val SPOUT_PATH = "sd.spout.path"
    val PARSER_VALUE_FIELD = "sd.parser.value_field"
    val MOVING_AVERAGE_WINDOW = "sd.moving_average.window"
    val SPIKE_DETECTOR_THRESHOLD = "sd.spike_detector.threshold"
  }

  object Component {
    val MOVING_AVERAGE = "moving_average"
    val SPIKE_DETECTOR = "spike_detector"
  }


  object Field {
    val DEVICE_ID = "deviceID"
    val VALUE = "value"
    val MOVING_AVG = "movingAverage"
  }


  object DatasetParsing {
    val DateField = 0
    val TimeField = 1
    val EpochField = 2
    val DeviceIdField = 3
    val TempField = 4
    val HumidField = 5
    val LightField = 6
    val VoltField = 7
  }
}


