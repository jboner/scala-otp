/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.mnesia

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

