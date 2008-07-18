package scala.binary

/**
 * A Binary object composed of two child Binary objects.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
final class CompositeBinary private[binary] (private[this] val left: Binary, private[this] val right: Binary) extends Binary {

  override val length = left.length + right.length

  private[this] var cachedHashCode = 0

  override def apply(i: Int) = if (i < left.length) left(i) else right(i - left.length)

  override def hashCode = {
    if (cachedHashCode == 0) cachedHashCode = super.hashCode
    cachedHashCode
  }

  override def copyToByteArray(srcOffset: Int, array: Array[Byte], destOffset: Int, copyLength: Int): Unit = {
    import Math._
    if ((srcOffset + copyLength) > this.length || (destOffset + copyLength) > array.length)
      throw new IndexOutOfBoundsException
    val leftCopyLength = max(min(copyLength, left.length - srcOffset), 0)
    if (leftCopyLength > 0) {
      left.copyToByteArray(srcOffset, array, destOffset, leftCopyLength)
    }
    val rightCopyLength = copyLength - leftCopyLength
    if (rightCopyLength > 0) {
      right.copyToByteArray(srcOffset + leftCopyLength - left.length, array, destOffset + leftCopyLength, rightCopyLength)
    }
  }

}
