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

trait AsyncWritable {

  private val writeLock = new AsyncLock()

  protected def internalWrite(binary: Binary)(fc: FC[Unit]): Nothing

  final def asyncWrite(binary: Binary)(fc: FC[Unit]): Nothing = {
    writeLock.syn(internalWrite(binary)(_: FC[Unit]))(fc)
  }

  final private def asyncWrite(as: AsyncStream[Binary])(fc: FC[Unit]): Nothing = {
    (writeLock.syn { (fc2: FC[Unit]) =>
      as.asyncForeach(internalWrite(_: Binary)(_: FC[Unit]))(fc2)
    })(fc)
  }
}
