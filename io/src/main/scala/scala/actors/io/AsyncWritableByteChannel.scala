package scala.actors.io

import java.nio.channels._
import java.nio.ByteBuffer
import scala.actors.controlflow._
import scala.actors.controlflow.AsyncStreamBuilder
import scala.actors.controlflow.ControlFlow._
import scala.actors.controlflow.concurrent.AsyncLock
import scala.binary.Binary
import scala.collection.immutable.Queue
import scala.collection.jcl.Conversions._

trait AsyncWritableByteChannel extends AsyncWritable {
  val channel: SelectableChannel with GatheringByteChannel
  val asyncSelector: AsyncSelector
  
  protected def internalWrite(binary: Binary)(fc: FC[Unit]): Nothing = {
    import fc.implicitThr
    def tryWrite(buffers: Array[ByteBuffer], offset: Int): Nothing = try {
      if (offset >= buffers.length) {
        fc.ret(())
      } else if (!buffers(offset).hasRemaining) {
        // Clean out empty or already-processed buffers.
        buffers(offset) = null // Allow garbage collection.
        tryWrite(buffers, offset + 1) // Avoid re-processing.
      } else {
        //println("Writing buffers: " + buffers.length)
        channel.write(buffers, offset, buffers.length) match {
          case 0 => {
            // Write failed, use selector to callback when ready.
            asyncSelector.register(channel, AsyncSelector.Write) { () => tryWrite(buffers, offset) }
          }
          case _ => {
            //println("AsyncWritableByteChannel: wrote "+length+" bytes.")
            tryWrite(buffers, offset)
          }
        }
      }
    } catch {
      case e: Exception => fc.thr(e)
    }
    tryWrite(binary.byteBuffers.toList.toArray, 0)
  }
}
