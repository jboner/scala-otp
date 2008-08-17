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
  def asyncRead(maxLength: Int)(fc: FC[Binary]): Nothing = {
    import fc.implicitThr
    val buffer = ByteBuffer.allocate(maxLength)
    def tryRead: Nothing = {
      channel.read(buffer) match {
        case -1 => fc.ret(Binary.empty)
        case 0 => {
          // No data available yet: set callback.
          //println("No bytes for reading: awaiting callback.") 
          richSelector.register(channel, RichSelector.Read) { () => tryRead }
          Actor.exit
        }
        case length => {
          // XXX: Could sometimes use array if length < maxLength - would save a copy.
          if (length == maxLength && buffer.hasArray) {
            fc.ret(Binary.fromArray(buffer.array, buffer.arrayOffset, buffer.capacity, false))
          } else {
            buffer.flip
            val array = new Array[Byte](buffer.remaining)
            buffer.get(array)
            fc.ret(Binary.fromArray(array, 0, array.length, false))
          }
        }
      }
    }
    tryRead
  }

  // warn about memory usage!
  def asyncReadAll(fc: FC[Binary]): Nothing = {
    import fc.implicitThr
    // XXX: Replace with asyncFold.
    def readAllLoop(accum: Binary): Nothing = {
      asyncRead(defaultReadLength) { binary: Binary =>
        if (binary.isEmpty) {
          fc.ret(accum)
        } else {
          readAllLoop(accum ++ binary)
        }
      }
    }
    readAllLoop(Binary.empty)
  }

  def asyncReadStream(maxBinaryLength: Int)(fc: FC[AsyncStream[Binary]]): Nothing = {
    import fc.implicitThr
    asyncRead(maxBinaryLength) { binary: Binary =>
      if (binary.isEmpty) {
        fc.ret(AsyncStream.empty)
      } else {
        fc.ret(AsyncStream.cons(binary, asyncReadStream(maxBinaryLength) _))
      }
    }
  }
}
