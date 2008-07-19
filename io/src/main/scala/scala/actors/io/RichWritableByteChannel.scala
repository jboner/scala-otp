package scala.actors.io

import java.nio.channels._
import java.nio.ByteBuffer
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.binary.Binary
import scala.collection.immutable.Queue
import scala.collection.jcl.Conversions._

trait RichWritableByteChannel {
  val channel: SelectableChannel with WritableByteChannel
  val richSelector: RichSelector

  protected[this] def createDefaultBuffer: ByteBuffer

  def asyncWrite(binary: Binary)(k: Cont[Unit]): Nothing =
    asyncWriteWithBuffer(binary, createDefaultBuffer)(k)

  def asyncWriteWithBuffer(binary: Binary, buffer: ByteBuffer)(k: Cont[Unit]): Nothing = {
    import k.exceptionHandler
    var remaining = binary
    // Force !buffer.hasRemaining, so that it is filled with binary.
    buffer.position(0)
    buffer.limit(0)
    def asyncWrite0: Nothing = {
      if (remaining.length == 0) { k(()) }
      if (!buffer.hasRemaining) {
        buffer.clear
        var forBuffer: Binary = null
        if (buffer.remaining < remaining.length) {
          forBuffer = remaining.slice(0, buffer.remaining) // XXX: Efficiency?
          remaining = remaining.slice(buffer.remaining, remaining.length)
        } else {
          forBuffer = remaining
          remaining = Binary.empty
        }
        val array = forBuffer.toArray // XXX: Efficiency?
        //println("RichWritableByteChannel: populating intermediate buffer with "+(array.length)+" bytes.")
        buffer.put(array)
        buffer.flip
      }
      channel.write(buffer) match {
        case 0 => {
          // Write failed, use selector to callback when ready.
          //println("RichWritableByteChannel: write buffer not ready, waiting.")
          richSelector.register(channel, RichSelector.Write) { () => asyncWrite0 }
          Actor.exit
        }
        case length => {
          //println("RichWritableByteChannel: wrote "+length+" bytes.")
          asyncWrite0
        }
      }
    }
    asyncWrite0
  }

  // XXX: Add writeAll() with AsyncStream?

}
