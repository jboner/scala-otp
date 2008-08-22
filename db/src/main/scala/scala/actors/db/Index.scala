/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.db

import java.util.concurrent.atomic.AtomicLong

/**
 * Primary key base class.
 * 
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
@serializable
case class PK(override val value: Long) extends Index {
  type T = Long
  var table: Class[_] = _ // FIXME: should not be mutable
  def compare(that: PK): Int = this.value.toInt - that.value.toInt
}

/**
 * Implicit definitions converting Scala primitive types to Indexes and vice versa 
 * (also handles PK conversions: PK <--> Long)
 * <p/>
 * Import into user classes in order to avoid explicit Index management:
 * <pre>
 * import com.triental.gps.caching.Index._
 * </pre>
 *
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object Index {

  implicit def longToPK(value: Long): PK = PK(value)
  implicit def pkToLong(pk: PK): Long = pk.value

  implicit def stringToIndex(value: String): StringIndex = StringIndex(value)
  implicit def indexToString(index: StringIndex): String = index.value

  implicit def intToIndex(value: Int): IntIndex = IntIndex(value)
  implicit def indexToInt(index: IntIndex): Int = index.value

  implicit def longToIndex(value: Long): LongIndex = LongIndex(value)
  implicit def indexToLong(index: LongIndex): Long = index.value

  implicit def floatToIndex(value: Float): FloatIndex = FloatIndex(value)
  implicit def indexToFloat(index: FloatIndex): Float = index.value

  implicit def doubleToIndex(value: Double): DoubleIndex = DoubleIndex(value)
  implicit def indexToDouble(index: DoubleIndex): Double = index.value

  implicit def charToIndex(value: Char): CharIndex = CharIndex(value)
  implicit def indexToChar(index: CharIndex): Char = index.value

}

abstract case class Index extends Ordered[Index] {
  type T
  val value: T
  def compare(that: Index): Int = this.hashCode - that.hashCode
  override def toString = "Index[" + value + "]"
}

case class StringIndex(override val value: String) extends Index {
  type T = String
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
  override def toString = "StringIndex[" + value + "]"
}

case class IntIndex(override val value: Int) extends Index {
  type T = Int
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
  override def toString = "IntIndex[" + value + "]"
}

case class LongIndex(override val value: Long) extends Index {
  type T = Long
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
  override def toString = "LongIndex[" + value + "]"
}

case class FloatIndex(override val value: Float) extends Index {
  type T = Float
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
  override def toString = "FloatIndex[" + value + "]"
}

case class DoubleIndex(override val value: Double) extends Index {
  type T = Double
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
  override def toString = "DoubleIndex[" + value + "]"
}

case class CharIndex(override val value: Char) extends Index {
  type T = Char
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
  override def toString = "CharIndex[" + value + "]"
}

