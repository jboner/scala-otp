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
  def compare(that: StringIndex): Int = this.hashCode - that.hashCode
  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, value)
    result
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object StringIndex {
  def newInstance: String => Index = (value: String) => StringIndex(value.asInstanceOf[String])
}
