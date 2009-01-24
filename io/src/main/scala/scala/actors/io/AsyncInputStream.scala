package scala.actors.io

import java.io.InputStream
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.binary.Binary

class AsyncInputStream(in: InputStream) extends AsyncReadable {
  if (in == null) throw new NullPointerException
  
  // returns zero-length Binary when reaches end
  final protected def internalRead(bufferLength: Int)(fc: FC[Binary]): Nothing = {
    try {
      val buffer = new Array[Byte](bufferLength)
      val lengthRead = try { in.read(buffer) } catch { case t => fc.thr(t) }
      if (lengthRead == -1) {
        fc.ret(Binary.empty)
      } else {
        fc.ret(Binary.fromArray(buffer, 0, lengthRead, false))
      }
    } catch {
      case e: Exception => fc.thr(e)
    }
  }

}
