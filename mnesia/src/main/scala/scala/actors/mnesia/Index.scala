/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.mnesia

/**
 * Implicit definitions converting Scala primitive types to Indexes and vice versa.
 * <p/>
 * Import into user classes in order to avoid explicit Index management: 
 * <pre>
 * import scala.actors.mnesia.Index._
 * </pre>
 *
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object Index {
  implicit def stringToIndex(value: String): StringIndex = StringIndex(value)
  implicit def indexToString(index: StringIndex): String = index.value

  implicit def shortToIndex(value: Short): ShortIndex = ShortIndex(value)
  implicit def indexToShort(index: ShortIndex): Short = index.value

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

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
abstract case class Index extends Ordered[Index] {
  type T
  val value: T
  def compare(that: Index): Int = this.hashCode - that.hashCode
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
case class StringIndex(override val value: String) extends Index {
  type T = String
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
case class ShortIndex(override val value: Short) extends Index {
  type T = Short
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
case class IntIndex(override val value: Int) extends Index {
  type T = Int
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
case class LongIndex(override val value: Long) extends Index {
  type T = Long
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
case class FloatIndex(override val value: Float) extends Index {
  type T = Float
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
case class DoubleIndex(override val value: Double) extends Index {
  type T = Double
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
case class CharIndex(override val value: Char) extends Index {
  type T = Char
  override def hashCode: Int = HashCode.hash(HashCode.SEED, value)
}
