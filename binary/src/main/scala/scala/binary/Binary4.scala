package scala.binary

/**
 * A Binary object containing exactly 4 bytes.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
final class Binary4(
  private[this] val b0: Byte,
  private[this] val b1: Byte,
  private[this] val b2: Byte,
  private[this] val b3: Byte
) extends Binary {

  override def length = 4

  override def apply(i: Int): Byte =
    i match {
      case 0 => b0
      case 1 => b1
      case 2 => b2
      case 3 => b3
      case _ => throw new IndexOutOfBoundsException
    }

}
