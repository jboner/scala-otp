package scala.binary

/**
 * A Binary object containing exactly 2 bytes.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
final class Binary2(
  private[this] val b0: Byte,
  private[this] val b1: Byte
) extends Binary {

  override def length = 2

  override def apply(i: Int): Byte =
    i match {
      case 0 => b0
      case 1 => b1
      case _ => throw new IndexOutOfBoundsException
    }

}
