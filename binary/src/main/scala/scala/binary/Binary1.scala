package scala.binary

/**
 * A Binary object containing exactly 1 byte.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
final class Binary1(
  private[this] val b0: Byte
) extends Binary {

  override def length = 1

  override def apply(i: Int): Byte =
    i match {
      case 0 => b0
      case _ => throw new IndexOutOfBoundsException
    }

}
