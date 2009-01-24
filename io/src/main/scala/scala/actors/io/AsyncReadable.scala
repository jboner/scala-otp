package scala.actors.io

import java.nio.channels._
import java.nio.ByteBuffer
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.actors.controlflow.concurrent._
import scala.binary.Binary
import scala.collection.immutable.Queue
import scala.collection.jcl.Conversions._

trait AsyncReadable {

  protected def defaultReadLength: Int = 512

  @volatile
  private var readStreamFuture: AsyncFuture[AsyncStream[Binary]] =
    new AsyncLazyFuture[AsyncStream[Binary]](nextReadStream _)

  private def nextReadStream(fc: FC[AsyncStream[Binary]]): Nothing = {
    import fc.implicitThr
    internalRead(defaultReadLength) { binary: Binary =>
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
  protected def internalRead(length: Int)(fc: FC[Binary]): Nothing

  final def asyncReadStream: AsyncFuture[AsyncStream[Binary]] = readStreamFuture

  final def asyncRead(fc: FC[Binary]): Nothing = asyncReadLength(defaultReadLength)(fc)

  final def asyncReadLength(length: Int)(fc: FC[Binary]): Nothing = internalRead(length)(fc)

  final def asyncReadAll(fc: FC[Binary]): Nothing = {
    import fc.implicitThr
    val append = ((bs: (Binary, Binary)) => bs._1 ++ bs._2).toAsyncFunction
    asyncReadStream { as: AsyncStream[Binary] =>
      as.asyncFoldLeft(Binary.empty)(append)(fc)
    }
  }
}
