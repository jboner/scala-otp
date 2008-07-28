package scala.actors.io

import java.io.InputStream
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.binary.Binary

class RichInputStream(in: InputStream) {

  def asyncRead(k: Cont[Option[Binary]]) = {
    val buffer = new Array[Byte](256)
    val lengthRead = try { in.read(buffer) } catch { case t => k.exception(t) }
    if (lengthRead == -1) {
      k(None)
    } else {
      val binary = Binary.fromSeq(buffer, 0, lengthRead)
      k(Some(binary))
    }
  }

  def asyncReadAll(k: Cont[AsyncStream[Binary]]): Nothing = {
    val buffer = new Array[Byte](256)
    def readNext(k: Cont[AsyncStream[Binary]]): Nothing = {
      val lengthRead = try { in.read(buffer) } catch { case t => k.exception(t) }
      if (lengthRead == -1) {
        k(AsyncStream.empty)
      } else {
        val binary = Binary.fromSeq(buffer, 0, lengthRead)
        k(AsyncStream.cons(binary, readNext _))
      }
    }
    readNext(k)
  }

}
