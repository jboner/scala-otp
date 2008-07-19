package scala.binary

/**
 * An empty Binary object.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
object Binary0 extends Binary {

  override def length = 0

  override def apply(i: Int): Byte = throw new IndexOutOfBoundsException

  // TODO: Add optimized methods for append, copyToArray, etc.

}
