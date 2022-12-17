
package Util.collections

import java.lang.reflect.Array
import java.util.Queue
import scala.collection.mutable


/**
 * real-time-traf storm.realTraffic.gis FixedSizeQueue.java
 *
 * Copyright 2013 Xdata@SIAT
 * Created:2013-4-8 下午3:26:36
 * email: gh.chen@siat.ac.cn
 *
 */
class FixedSizeQueue[E](private var capacity: Int) extends mutable.Queue[E] {
//  private var elements: Array = new Array(capacity)
//  private var head = 0
//  private var tail = (head - 1) % capacity;
//  private var size = 0
//
//  private var modCount = 0
//
//  override def add(e: E): Boolean = {
//    modCount += 1
//    tail = (tail + 1) % capacity
//    elements(tail) = e
//    size = if ((size + 1) > capacity) capacity
//    else size + 1
//    head = (tail + 1 + capacity - size) % capacity
//    true
//  }


}

