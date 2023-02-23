package TrafficMonitoring

import org.apache.spark.streaming.dstream.DStream

class MapMatching(ssc: DStream[(String, Double, Double, Int, Int, Long)], parDegree: Int, city: String) {
    def execute(): Unit = {

    }
}
