package scala.actors.io

import java.io.OutputStream
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.binary.Binary

class AsyncOutputStream(out: OutputStream) extends AsyncWritable {
  if (out == null) throw new NullPointerException
  
  final protected def internalWrite(binary: Binary)(fc: FC[Unit]): Nothing = {
    try {
      write(binary)
      fc.ret(())
    } catch {
      case e: Exception => fc.thr(e)
    }
  }

  final def write(binary: Binary): Unit = {
    for (arrayBinary <- binary.arrays) {
      out.write(arrayBinary.array, arrayBinary.offset, arrayBinary.length)
    }
  }

}
