package scala.binary

/**
 * A Binary object containing exactly 3 bytes.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
final class Binary3(
  private[this] val b0: Byte,
  private[this] val b1: Byte,
  private[this] val b2: Byte
) extends Binary {

  override def length = 3

  override def apply(i: Int): Byte =
    i match {
      case 0 => b0
      case 1 => b1
      case 2 => b2
      case _ => throw new IndexOutOfBoundsException
    }

}
