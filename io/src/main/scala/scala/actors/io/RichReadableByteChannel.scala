package scala.actors.io

import java.nio.channels._
import java.nio.ByteBuffer
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.binary.Binary
import scala.collection.immutable.Queue
import scala.collection.jcl.Conversions._

trait RichReadableByteChannel {
  val channel: SelectableChannel with ReadableByteChannel
  val richSelector: RichSelector

  protected[this] val bufferLength: Int

  /**
   * "Copies" a ByteBuffer into a Binary, consuming the buffer in the
   * process.  A new, equal-sized ByteBuffer is returned along with
   * the Binary. Returned ByteBuffers have a position of zero, a limit
   * set to the capacity and no mark - equivalent to calling clear.
   */
  private[this] def copy(buffer: ByteBuffer): (ByteBuffer, Binary) = {
    if (buffer.hasArray && buffer.remaining == buffer.capacity) {
      // Consume the ByteBuffer and create a new one.
      //println("Wrapping ByteBuffer directly in Binary.")
      val newBuffer = ByteBuffer.allocate(buffer.capacity)
      val binary = Binary.fromSeq(buffer.array, buffer.arrayOffset, buffer.capacity, false)
      (newBuffer, binary)
    } else {
      // Copy the ByteBuffer's contents.
      //println("Copying ByteBuffer contents into Binary.")
      val array = new Array[Byte](buffer.remaining)
      buffer.get(array)
      val binary = Binary.fromSeq(array, 0, array.length, false)
      buffer.clear
      (buffer, binary)
    }
  }

  // returns zero-length Binary when reaches end
  def asyncRead(k: Cont[Binary]): Nothing = {
    import k.exceptionHandler
    def tryRead(buffer: ByteBuffer, accum: Binary): Nothing = {
      buffer.clear
      channel.read(buffer) match {
        case -1 => k(accum)
        case 0 if accum.isEmpty => {
          // No data available yet: set callback.
          //println("No bytes for reading: awaiting callback.") 
          richSelector.register(channel, RichSelector.Read) { () => tryRead(buffer, accum) }
          Actor.exit
        }
        case 0 => {
          // Reached end of available data: sending accum.
          //println("No bytes for reading: ending read.")
          k(accum)
        }
        case length => {
          //println("Read "+length+" bytes.")
          buffer.flip
          val (newBuffer, binary) = copy(buffer)
          tryRead(newBuffer, accum ++ binary)
        }
      }
    }
    tryRead(ByteBuffer.allocate(bufferLength), Binary.empty)
  }

  // warn about memory usage!
  def asyncReadAll(k: Cont[Binary]): Nothing = {
    import k.exceptionHandler
    // XXX: Replace with asyncFold.
    def asyncReadAll0(accum: Binary): Nothing = {
      asyncRead { binary: Binary =>
        if (binary.isEmpty) {
          k(accum)
        } else {
          asyncReadAll0(accum ++ binary)
        }
      }
    }
    asyncReadAll0(Binary.empty)
  }

  def asyncReadStream(k: Cont[AsyncStream[Binary]]): Nothing = {
    import k.exceptionHandler
    asyncRead { binary: Binary =>
      if (binary.isEmpty) {
        k(AsyncStream.empty)
      } else {
        k(AsyncStream.cons(binary, asyncReadStream _))
      }
    }
  }
}
