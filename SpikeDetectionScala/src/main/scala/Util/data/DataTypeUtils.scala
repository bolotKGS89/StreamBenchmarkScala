package Util.data

object DataTypeUtils {
  def isInteger(str: String): Boolean = {
    if (str == null) return false
    val length = str.length
    if (length == 0) return false
    var i = 0
    if (str.charAt(0) == '-') {
      if (length == 1) return false
      i = 1
    }

    while (i < length) {
      val c = str.charAt(i)
      if (c <= '/' || c >= ':') return false

      i += 1
    }
    true
  }
}
