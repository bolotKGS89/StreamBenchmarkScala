package Util

object Log extends Serializable {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("WordCount")
}
