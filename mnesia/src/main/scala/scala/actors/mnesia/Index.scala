/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.mnesia

object Index {
  implicit def stringToIndex(str: String): StringIndex = StringIndex(str)
  implicit def indexToString(index: StringIndex): String = index.value
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
