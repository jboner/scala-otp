package scala.actors.mnesia

import java.lang.reflect.{Array => JArray}
import java.lang.{Float => JFloat, Double => JDouble}

/**
 * Implementation is a Scala port of the the code listed on this page:
 * http://www.javapractices.com/topic/TopicAction.do?Id=28
 * <p>
 * Collected methods which allow easy implementation of <code>hashCode</code>.
 *
 * Example:
 * <pre>
 *  override def hashCode: Int = {
 *    var result = HashCode.SEED
 *    //collect the contributions of various fields
 *    result = HashCode.hash(result, fPrimitive)
 *    result = HashCode.hash(result, fObject)
 *    result = HashCode.hash(result, fArray)
 *    result
 *  }
 * </pre>
 */
object HashCode {
  val SEED = 23

  def hash(seed: Int, value: Boolean): Int = firstTerm(seed) + (if (value) 1 else 0)
  def hash(seed: Int, value: Char): Int = firstTerm(seed) + value.asInstanceOf[Int]
  def hash(seed: Int, value: Int): Int = firstTerm(seed) + value
  def hash(seed: Int, value: Long): Int = firstTerm(seed)  + (value ^ (value >>> 32) ).asInstanceOf[Int]
  def hash(seed: Int, value: Float): Int = hash(seed, JFloat.floatToIntBits(value))
  def hash(seed: Int, value: Double): Int = hash(seed, JDouble.doubleToLongBits(value))
  def hash(seed: Int, anyRef: AnyRef): Int = {
    var result = seed
    if (anyRef == null) result = hash(result, 0)
    else if (!isArray(anyRef)) result = hash(result, anyRef.hashCode())
    else { // is an array
      for (id <- 0 until JArray.getLength(anyRef)) {
        val item = JArray.get(anyRef, id)
        result = hash(result, item)
      }
    }
    result
  }
  private def firstTerm(seed: Int): Int = PRIME * seed
  private def isArray(anyRef: AnyRef): Boolean = anyRef.getClass.isArray
  private val PRIME = 37
}

