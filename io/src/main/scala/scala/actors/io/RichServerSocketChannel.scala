package scala.actors.io

import java.net.SocketAddress
import java.nio.channels._
import java.nio.ByteBuffer
import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.binary.Binary
import scala.collection.immutable.Queue
import scala.collection.jcl.Conversions._

class RichServerSocketChannel(val channel: ServerSocketChannel, val richSelector: RichSelector) {

  def asyncAccept(fc: FC[SocketChannel]): Nothing = {
    import fc.implicitThr
    def asyncAccept0: Nothing = {
      channel.accept match {
        case null => {
          // Accept failed, use selector to callback when ready.
          richSelector.register(channel, RichSelector.Accept) { () => asyncAccept0 }
          Actor.exit
        }
        case socketChannel => fc.ret(socketChannel)
      }
    }
    asyncAccept0
  }

}
