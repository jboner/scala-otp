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

  protected[this] def defaultReadLength: Int = 256

  // returns zero-length Binary when reaches end
  def asyncRead(maxLength: Int)(k: Cont[Binary]): Nothing = {
    import k.exceptionHandler
    val buffer = ByteBuffer.allocate(maxLength)
    def tryRead: Nothing = {
      channel.read(buffer) match {
        case -1 => k(Binary.empty)
        case 0 => {
          // No data available yet: set callback.
          //println("No bytes for reading: awaiting callback.") 
          richSelector.register(channel, RichSelector.Read) { () => tryRead }
          Actor.exit
        }
        case length => {
          // XXX: Could sometimes use array if length < maxLength - would save a copy.
          if (length == maxLength && buffer.hasArray) {
            k(Binary.fromSeq(buffer.array, buffer.arrayOffset, buffer.capacity, false))
          } else {
            buffer.flip
            val array = new Array[Byte](buffer.remaining)
            buffer.get(array)
            k(Binary.fromSeq(array, 0, array.length, false))
          }
        }
      }
    }
    tryRead
  }

  // warn about memory usage!
  def asyncReadAll(k: Cont[Binary]): Nothing = {
    import k.exceptionHandler
    // XXX: Replace with asyncFold.
    def readAllLoop(accum: Binary): Nothing = {
      asyncRead(defaultReadLength) { binary: Binary =>
        if (binary.isEmpty) {
          k(accum)
        } else {
          readAllLoop(accum ++ binary)
        }
      }
    }
    readAllLoop(Binary.empty)
  }

  def asyncReadStream(maxBinaryLength: Int)(k: Cont[AsyncStream[Binary]]): Nothing = {
    import k.exceptionHandler
    asyncRead(maxBinaryLength) { binary: Binary =>
      if (binary.isEmpty) {
        k(AsyncStream.empty)
      } else {
        k(AsyncStream.cons(binary, asyncReadStream(maxBinaryLength) _))
      }
    }
  }
}
