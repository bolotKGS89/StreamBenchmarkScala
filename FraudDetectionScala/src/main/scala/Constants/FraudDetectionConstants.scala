package Constants

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

/**
 * @author Bolot Kasybekov
 * @version May 2019
 *
 *          Constants peculiar of the FraudDetection application.
 */
object FraudDetectionConstants extends BaseConstants {
  val DEFAULT_MODEL = "frauddetection/model.txt"
  val DEFAULT_PROPERTIES = "frauddetection/fd.properties"
  val DEFAULT_TOPO_NAME = "FraudDetection"
  val RUNTIME = "fd.runtime_sec"
  val BUFFER_SIZE = "fd.buffer_size"
  val POLLING_TIME = "fd.polling_time_ms"
  val SPOUT_PATH = "fd.spout.path"
  val PREDICTOR_MODEL = "fd.predictor.model"
  val MARKOV_MODEL_KEY = "fd.markov.model.key"
  val LOCAL_PREDICTOR = "fd.local.predictor"
  val STATE_SEQ_WIN_SIZE = "fd.state.seq.window.size"
  val STATE_ORDINAL = "fd.state.ordinal"
  val DETECTION_ALGO = "fd.detection.algorithm"
  val METRIC_THRESHOLD = "fd.metric.threshold"

//  object Conf {
//
//  }

  object Component {
    val PREDICTOR = "fraud_predictor"
  }

//  trait Component extends BaseConstants.BaseComponent {}

  object Field {
    val ENTITY_ID = "entityID"
    val RECORD_DATA = "recordData"
    val SCORE = "score"
    val STATES = "states"
  }

}