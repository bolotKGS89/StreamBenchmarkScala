/** ************************************************************************************
 * Copyright (c) 2019- Gabriele Mencagli and Alessandra Fais
 *
 * This file is part of StreamBenchmarks.
 *
 * StreamBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 * You can redistribute it and/or modify it under the terms of the
 * * GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version
 * OR
 * * MIT License: https://github.com/ParaGroup/StreamBenchmarks/blob/master/LICENSE.MIT
 *
 * StreamBenchmarks is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License and
 * the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 * and <http://opensource.org/licenses/MIT/>.
 * *************************************************************************************
 */

package Util.data

/**
 * Generic Pair class
 *
 * @author pranab
 * @param < L>
 * @param < R>
 */
class Pair[L, R]() {
  protected var left: L = _
  protected var right: R = _

  def this(left: L, right: R) {
    this()
    this.left = left
    this.right = right
  }

  def getLeft: L = left

  def setLeft(left: L): Unit = {
    this.left = left
  }

  def getRight: R = right

  def setRight(right: R): Unit = {
    this.right = right
  }

  override def hashCode: Int = left.hashCode ^ right.hashCode

  override def equals(other: Any): Boolean = {
    var isEqual = false
    if (null != other && other.isInstanceOf[Pair[_, _]]) {
      val pairOther = other.asInstanceOf[Pair[_, _]]
      isEqual = this.left == pairOther.left && this.right == pairOther.right
    }
    isEqual
  }
}
