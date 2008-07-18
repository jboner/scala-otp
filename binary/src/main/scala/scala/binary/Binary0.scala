package scala.binary

/**
 * An empty Binary object.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
final class Binary0 extends Binary {

  override def length = 0

  override def apply(i: Int): Byte = throw new IndexOutOfBoundsException

}
