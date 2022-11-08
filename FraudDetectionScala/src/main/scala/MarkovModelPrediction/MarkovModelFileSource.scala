package MarkovModelPrediction

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

class MarkovModelFileSource() extends IMarkovModelSource {
  private val charset = Charset.defaultCharset


  override def getModel(key: String): String = {
    var encoded: Array[Byte] = null
    try {
      encoded = Files.readAllBytes(Paths.get(key))
      charset.decode(ByteBuffer.wrap(encoded)).toString
    } catch {
      case ex: IOException =>
        null
    }
  }
}
