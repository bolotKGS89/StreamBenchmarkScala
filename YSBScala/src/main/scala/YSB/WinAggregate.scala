package YSB

import Util.Log
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

class WinAggregate {
  def doWinAggregate(stream: DStream[(String, String, Long)], parDegree: Int, initialTime: Long): DStream[(Any, Any, Any)] = {
    var processed = 0


    stream.transform({ rdd =>
      var discarded = 0

        rdd.repartition(parDegree)
          .map { case (cmp_id, ad_id, ts) => (cmp_id, (ad_id, ts)) }
          .groupByKey
          .flatMapValues { iter =>
            var winSet = Map.empty[Long, Long]
            var count = 0L
            var start_ts = initialTime
            var end_ts = (initialTime + 10e09).toLong
            var processed = 0

            iter.foreach { case (_, ts) =>
              if (ts >= end_ts) {
                count = 1
                start_ts = ((((ts - initialTime) / 10e09) * 10e09) + initialTime).toLong;
                end_ts = (((((ts - initialTime) / 10e09) + 1) * 10e09) + initialTime).toLong
                processed += 1
              } else if (ts >= start_ts) {
                count += 1
                processed += 1
              } else {
                discarded += 1
              }

              if (count > 0)
                winSet += count -> ts

            }

            winSet.map { case (count, ts) =>
              (count, ts)
            }
          }
          .groupByKey()
          .mapValues { iter =>
            iter.maxBy(_._1)
          }
          .map { case (cmpId, (count, ts)) => (cmpId, count, ts) }
      })

//      rdd.repartition(parDegree).mapPartitions(iter => {
//        val winSet = scala.collection.mutable.Map[String, Window]()
//        iter.map({ case (cmp_id, _, ts) => {
//          if (winSet.contains(cmp_id)) {
//            // get the current window of key cmp_id
//            var win = winSet(cmp_id)
//            if (ts >= win.end_ts) { // window is triggered
//              val window = (cmp_id, win.count, ts)
//              win = new Window(1,
//                ((((ts - initialTime) / 10e09) * 10e09) + initialTime).toLong,
//                (((((ts - initialTime) / 10e09) + 1) * 10e09) + initialTime).toLong)
//              winSet(cmp_id) = win
//              processed += 1
//              window
//            }
//            else if (ts >= win.start_ts) { // window is not triggered
//              win.count += 1
//              processed += 1
//              (cmp_id, win.count, ts)
//            }
//            else { // tuple belongs to a previous already triggered window -> it is discarded!
//              discarded += 1
//              null
//            }
//          } else {
//            val win = new Window(1, initialTime, (initialTime + 10e09).toLong)
//            winSet += (cmp_id -> win)
//            null
//          }
//        }
//        }).filter(_ != null)
//
//      })
//    })


      //      val endTime = System.nanoTime()
      //      val latency = endTime - startTime // Measure the time it took to process the data
      //      Log.log.warn(s"[WinAggregate] latency: $latency")
      //
      //      val elapsedTime = (endTime - startTime) / 1000000000.0
      //      val tuples: Double = (processed / elapsedTime).toDouble
      //      val formattedTuples = String.format("%.5f", tuples)
      //      Log.log.info(s"[WinAggregate] bandwidth: $formattedTuples tuples/sec")

    }
}
