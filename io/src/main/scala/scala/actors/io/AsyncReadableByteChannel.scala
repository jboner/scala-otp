package scala.actors.io

import java.nio.channels._
import java.nio.ByteBuffer
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.actors.controlflow.concurrent._
import scala.binary.Binary
import scala.collection.immutable.Queue
import scala.collection.jcl.Conversions._

trait AsyncReadableByteChannel extends AsyncReadable {
  val channel: SelectableChannel with ReadableByteChannel
  val asyncSelector: AsyncSelector
  
  // returns zero-length Binary when reaches end
  final protected def internalRead(bufferLength: Int)(fc: FC[Binary]): Nothing = {
    import fc.implicitThr
    // XXX: Optimise by reusing buffer between invocations?
    val buffer = ByteBuffer.allocate(bufferLength)
    def tryRead: Nothing = {
      try {
        channel.read(buffer) match {
          case -1 => fc.ret(Binary.empty)
          case 0 => {
            // No data available yet: set callback.
            //println("No bytes for reading: awaiting callback.") 
            asyncSelector.register(channel, AsyncSelector.Read) { () => tryRead }
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
      } catch {
        case e: Exception => fc.thr(e)
      }
    }
    tryRead
  }
}
