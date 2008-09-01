package scala.actors.io

import java.nio.channels._
import java.nio.ByteBuffer
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.actors.controlflow.concurrent._
import scala.binary.Binary
import scala.collection.immutable.Queue
import scala.collection.jcl.Conversions._

trait RichReadableByteChannel {
  val channel: SelectableChannel with ReadableByteChannel
  val richSelector: RichSelector
  
  protected def readLength: Int

  @volatile
  private var readStreamFuture: AsyncFuture[AsyncStream[Binary]] =
    new AsyncLazyFuture[AsyncStream[Binary]](nextReadStream _)

  private def nextReadStream(fc: FC[AsyncStream[Binary]]): Nothing = {
    import fc.implicitThr
    internalRead { binary: Binary =>
      if (binary.isEmpty) {
        fc.ret(AsyncStream.empty)
      } else {
        val stream = AsyncStream.cons(binary, nextReadStream _)
        readStreamFuture = stream.asyncTail
        fc.ret(stream)
      }
    }
  }

  // returns zero-length Binary when reaches end
  private def internalRead(fc: FC[Binary]): Nothing = {
    import fc.implicitThr
    // XXX: Optimise by reusing buffer between invocations?
    val bufferLength = readLength
    val buffer = ByteBuffer.allocate(bufferLength)
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
          if (length == bufferLength && buffer.hasArray) {
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

  def asyncReadStream: AsyncFuture[AsyncStream[Binary]] = readStreamFuture

  def asyncRead(fc: FC[Binary]): Nothing = internalRead(fc)

  def asyncReadAll(fc: FC[Binary]): Nothing = {
    import fc.implicitThr
    val append = ((bs: (Binary, Binary)) => bs._1 ++ bs._2).toAsyncFunction
    asyncReadStream { as: AsyncStream[Binary] =>
      as.asyncFoldLeft(Binary.empty)(append)(fc)
    }
  }
}
