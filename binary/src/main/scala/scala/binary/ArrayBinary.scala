package scala.binary

/**
 * A Binary object backed by an Array.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
final class ArrayBinary private[binary] (private[this] val array: Array[Byte]) extends Binary {

  private[this] var cachedHashCode = 0

  override def length = array.length

  override def apply(i: Int) = array(i)

  override def hashCode = {
    if (cachedHashCode == 0) cachedHashCode = super.hashCode
    cachedHashCode
  }

  override def copyToByteArray(srcOffset: Int, array: Array[Byte], destOffset: Int, copyLength: Int): Unit =
    Array.copy(this.array, srcOffset, array, destOffset, copyLength)

}
