package scala.actors.io

import java.nio.channels._
import java.nio.ByteBuffer
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.binary.Binary
import scala.collection.immutable.Queue
import scala.collection.jcl.Conversions._

trait RichWritableByteChannel {
  val channel: SelectableChannel with GatheringByteChannel
  val richSelector: RichSelector

  def asyncWrite(binary: Binary)(k: Cont[Unit]): Nothing = {
    import k.exceptionHandler
    def tryWrite(buffers: Array[ByteBuffer], offset: Int): Nothing = try {
      if (offset >= buffers.length) {
        k(())
      } else if (!buffers(offset).hasRemaining) {
        // Clean out empty or already-processed buffers.
        buffers(offset) = null // Allow garbage collection.
        tryWrite(buffers, offset + 1) // Avoid re-processing.
      } else {
        //println("Writing buffers: " + buffers.length)
        channel.write(buffers, offset, buffers.length) match {
          case 0 => {
            // Write failed, use selector to callback when ready.
            richSelector.register(channel, RichSelector.Write) { () => tryWrite(buffers, offset) }
            Actor.exit
          }
          case _ => {
            //println("RichWritableByteChannel: wrote "+length+" bytes.")
            tryWrite(buffers, offset)
          }
        }
      }
    } catch {
      case e: Exception => k.exception(e)
    }
    tryWrite(binary.toByteBuffers.toArray, 0)
  }
}
