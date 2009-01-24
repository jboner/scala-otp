package scala.binary

import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.nio.ByteBuffer

/**
 * A Binary object backed by an Array.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
@serializable
@SerialVersionUID(-8770946590245372965L)
private[scala] final case class ArrayBinary private[binary] (private[scala] var array: Array[Byte], private[scala] var offset: Int, var length: Int) extends Binary with Serializable {

  override private[binary] def depth = 0

  override def apply(i: Int) = {
    if (i < 0 || i >= length) throw new IndexOutOfBoundsException
    array(offset + i)
  }

  protected def forcedSlice0(from: Int, until: Int): ArrayBinary = {
    new ArrayBinary(array, offset + from, until - from)
  }

  protected def copyToArray0(from: Int, until: Int, dest: Array[Byte], destFrom: Int): Unit = {
    //println("ArrayBinary"+(this.array, offset, length)+".copyToArray0"+(from: Int, until: Int, dest: Array[Byte], destFrom: Int))
    //println("Array.copy" + (this.array, offset + from, array, destFrom, until - from))
    Array.copy(this.array, offset + from, dest, destFrom, until - from)
  }

  protected def arrays0(from: Int, until: Int): Iterable[ArrayBinary] = forcedSlice0(from, until) :: Nil

  private[scala] def wrappingByteBuffer: ByteBuffer = ByteBuffer.wrap(array, offset, length)

  override def elements: Iterator[Byte] = array.slice(offset, offset + length).elements

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeInt(length)
    out.write(array, offset, length)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    offset = 0
    length = in.readInt()
    array = new Array[Byte](length)
    var remaining = length
    while (remaining > 0) {
      val readLength = in.read(array, length - remaining, remaining)
      if (readLength == -1) {
        throw new IOException("Expected " + remaining + " more bytes.")
      }
      remaining -= readLength
    }
  }

}
