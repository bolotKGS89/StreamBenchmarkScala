package MarkovModelPrediction

import com.google.common.io.Resources

import java.io.IOException
import java.nio.charset.Charset

class MarkovModelResourceSource() extends IMarkovModelSource {
  private val charset = Charset.defaultCharset


  override def getModel(key: String): String = try {
    val url = Resources.getResource(key)
    Resources.toString(url, charset)
  } catch {
    case ex: IOException =>
      null
  }
}
