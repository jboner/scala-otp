package scala.actors.io

import java.io.InputStream
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.binary.Binary

class RichInputStream(in: InputStream) {

  def asyncRead(fc: FC[Option[Binary]]) = {
    val buffer = new Array[Byte](256)
    val lengthRead = try { in.read(buffer) } catch { case t => fc.thr(t) }
    if (lengthRead == -1) {
      fc.ret(None)
    } else {
      val binary = Binary.fromArray(buffer, 0, lengthRead)
      fc.ret(Some(binary))
    }
  }

  def asyncReadAll(fc: FC[AsyncStream[Binary]]): Nothing = {
    val buffer = new Array[Byte](256)
    def readNext(fc: FC[AsyncStream[Binary]]): Nothing = {
      val lengthRead = try { in.read(buffer) } catch { case t => fc.thr(t) }
      if (lengthRead == -1) {
        fc.ret(AsyncStream.empty)
      } else {
        val binary = Binary.fromArray(buffer, 0, lengthRead)
        fc.ret(AsyncStream.cons(binary, readNext _))
      }
    }
    readNext(fc)
  }

}
