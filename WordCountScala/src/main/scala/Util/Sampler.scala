package Util


object Sampler extends java.io.Serializable// constructor
{
  private var samples: List[Double] = List()
  private val epoch: Long = System.nanoTime
  private var counter: Long = 0L
  private var total: Long = 0L
  private var samplesPerSeconds: Long = 0L// initialize beforehand

  def init(samplesPerSeconds: Long): Unit = {
    this.samplesPerSeconds = samplesPerSeconds
  }

  // add method
  def add(value: Double): Unit = {
    add(value, 0)
  }

  def add(value: Double, timestamp: Long): Unit = {
    total += 1
    // add samples according to the sample rate
    val seconds: Double = (timestamp - epoch) / 1e9
    if (samplesPerSeconds == 0 || counter <= samplesPerSeconds * seconds) {
      samples = value :: samples
      counter += 1
    }
  }

  // getValues method
  def getValues: List[Double] = samples

  // getTotal method
  def getTotal: Long = total
}