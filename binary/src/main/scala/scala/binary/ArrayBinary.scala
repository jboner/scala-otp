package scala.binary

import java.nio.ByteBuffer

/**
 * A Binary object backed by an Array.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
private[scala] final case class ArrayBinary private[binary] (private[scala] val array: Array[Byte], private[scala] val offset: Int, val length: Int) extends Binary {

  override private[binary] def depth = 0

  override def apply(i: Int) = {
    if (i < 0 || i >= length) throw new IndexOutOfBoundsException
    array(offset + i)
  }

  protected def slice0(from: Int, until: Int): ArrayBinary = {
    new ArrayBinary(array, offset + from, until - from)
  }

  protected def copyToArray0(from: Int, until: Int, dest: Array[Byte], destFrom: Int): Unit = {
    //println("ArrayBinary"+(this.array, offset, length)+".copyToArray0"+(from: Int, until: Int, dest: Array[Byte], destFrom: Int))
    //println("Array.copy" + (this.array, offset + from, array, destFrom, until - from))
    Array.copy(this.array, offset + from, dest, destFrom, until - from)
  }

  protected def arrays0(from: Int, until: Int): Iterable[ArrayBinary] = slice0(from, until) :: Nil

  private[scala] def wrappingByteBuffer: ByteBuffer = ByteBuffer.wrap(array, offset, length)

}
