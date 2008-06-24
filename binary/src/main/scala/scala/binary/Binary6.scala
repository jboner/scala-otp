package scala.binary

/**
 * A Binary object containing exactly 6 bytes.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
final class Binary6(
  private[this] val b0: Byte,
  private[this] val b1: Byte,
  private[this] val b2: Byte,
  private[this] val b3: Byte,
  private[this] val b4: Byte,
  private[this] val b5: Byte
) extends Binary {

  override def length = 6

  override def apply(i: Int): Byte =
    i match {
      case 0 => b0
      case 1 => b1
      case 2 => b2
      case 3 => b3
      case 4 => b4
      case 5 => b5
      case _ => throw new IndexOutOfBoundsException
    }

}
