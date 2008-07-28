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

  protected[this] def createDefaultBuffer: ByteBuffer

  // returns zero-length Binary when reach end
  def asyncRead(k: Cont[Binary]): Nothing =
    asyncReadWithBuffer(createDefaultBuffer)(k)

  // returns zero-length Binary when reach end
  def asyncReadWithBuffer(buffer: ByteBuffer)(k: Cont[Binary]): Nothing = {
    import k.exceptionHandler
    def asyncRead0: Nothing = {
      buffer.clear
      channel.read(buffer) match {
        case -1 => k(Binary.empty)
        case 0 => {
          // Read failed, use selector to callback when ready.
          richSelector.register(channel, RichSelector.Read) { () => asyncRead0 }
          Actor.exit
        }
        case length => {
          buffer.flip
          // XXX: May be able to avoid copying into array in some cases.
          val array = new Array[Byte](buffer.remaining)
          buffer.get(array)
          val binary = Binary.fromSeq(array, 0, length, false)
          k(binary)
        }
      }
    }
    asyncRead0
  }

  def asyncReadAll(k: Cont[Binary]): Nothing =
    asyncReadAllWithBuffer(createDefaultBuffer)(k)

  def asyncReadAllWithBuffer(buffer: ByteBuffer)(k: Cont[Binary]): Nothing = {
    import k.exceptionHandler
    // XXX: Replace with asyncFold.
    def asyncReadAll0(accum: Binary): Nothing = {
      asyncReadWithBuffer(buffer) { binary: Binary =>
        if (binary.isEmpty) {
          k(accum)
        } else {
          asyncReadAll0(accum ++ binary)
        }
      }
    }
    asyncReadAll0(Binary.empty)
  }

  def asyncReadStream(k: Cont[AsyncStream[Binary]]): Nothing =
    asyncReadStreamWithBuffer(createDefaultBuffer)(k)

  def asyncReadStreamWithBuffer(buffer: ByteBuffer)(k: Cont[AsyncStream[Binary]]): Nothing = {
    import k.exceptionHandler
    asyncReadWithBuffer(buffer) { binary: Binary =>
      if (binary.isEmpty) {
        k(AsyncStream.empty)
      } else {
        k(AsyncStream.cons(binary, asyncReadStreamWithBuffer(buffer) _))
      }
    }
  }
}
