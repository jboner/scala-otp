package scala.binary

/**
 * A Binary object containing exactly 7 bytes.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
final class Binary7(
  private[this] val b0: Byte,
  private[this] val b1: Byte,
  private[this] val b2: Byte,
  private[this] val b3: Byte,
  private[this] val b4: Byte,
  private[this] val b5: Byte,
  private[this] val b6: Byte
) extends Binary {

  override def length = 7

  override def apply(i: Int): Byte =
    i match {
      case 0 => b0
      case 1 => b1
      case 2 => b2
      case 3 => b3
      case 4 => b4
      case 5 => b5
      case 6 => b6
      case _ => throw new IndexOutOfBoundsException
    }

}
