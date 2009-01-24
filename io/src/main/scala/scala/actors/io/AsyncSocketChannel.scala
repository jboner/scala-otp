package scala.actors.io

import java.net.SocketAddress
import java.nio.channels._
import java.nio.ByteBuffer
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.binary.Binary
import scala.collection.immutable.Queue
import scala.collection.jcl.Conversions._

class AsyncSocketChannel(val channel: SocketChannel, val asyncSelector: AsyncSelector) extends AsyncReadableByteChannel with AsyncWritableByteChannel {

  @volatile
  var readLength: Int = 256

  def asyncConnect(remote: SocketAddress)(fc: FC[Unit]): Nothing = {
    import fc.implicitThr
    channel.connect(remote)
    def asyncConnect0: Nothing = {
      channel.finishConnect match {
        case true => fc.ret(())
        case false => {
          // Connect failed, use selector to callback when ready.
          asyncSelector.register(channel, AsyncSelector.Connect) { () => asyncConnect0 }
        }
      }
    }
    asyncConnect0
  }

}
