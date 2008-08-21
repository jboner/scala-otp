/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.behavior

import scala.actors._
import scala.actors.Actor._

import org.testng.annotations.{Test, BeforeMethod}

import org.scalatest.testng.TestNGSuite
import org.scalatest._

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class GenericServerContainerSuite extends TestNGSuite {

  var inner: GenericServerContainerActor = null
  var server: GenericServerContainer = null
  def createProxy(f: () => GenericServer) = { val server = new GenericServerContainer("server", f); server.setTimeout(100); server }

  override protected def runTest(testName: String, reporter: Reporter, stopper: Stopper, properties: Map[String, Any]) {
    setup
    super.runTest(testName, reporter, stopper, properties)
  }

  @BeforeMethod
  def setup = {
    inner = new GenericServerContainerActor
    server = createProxy(() => inner)
    server.newServer
    server.start
  }

  @Test
  def testInit = {
    server.init("testInit")
    Thread.sleep(100)
    expect("initializing: testInit") {
      inner.log
    }
  }

  @Test
  def testTerminateWithReason = {
    server.terminate("testTerminateWithReason", 100)
    Thread.sleep(100)
    expect("terminating: testTerminateWithReason") {
      inner.log
    }
  }

  @Test
  def test_bang_1 = {
    server ! OneWay
    Thread.sleep(100)
    expect("got a oneway") {
      inner.log
    }
  }

  @Test
  def test_bang_2 = {
    server ! Ping
    Thread.sleep(100)
    expect("got a ping") {
      inner.log
    }
  }

  @Test
  def test_bangbangbang = {
    expect("pong") {
      (server !!! Ping).getOrElse("nil")
    }
    expect("got a ping") {
      inner.log
    }
  }

  @Test
  def test_bangquestion = {
    expect("pong") {
      val res: String = server !? Ping
      res
    }
    expect("got a ping") {
      inner.log
    }
  }

  @Test
  def test_bangbangbang_Timeout1 = {
    expect("pong") {
      (server !!! Ping).getOrElse("nil")
    }
    expect("got a ping") {
      inner.log
    }
  }

  @Test
  def test_bangbangbang_Timeout2 = {
    expect("error handler") {
      server !!! (OneWay, "error handler")
    }
    expect("got a oneway") {
      inner.log
    }
  }

  @Test
  def test_bangbangbang_GetFutureTimeout1 = {
    val future = server !! Ping
    future.receiveWithin(100) match {
      case None => fail("timed out") // timed out
      case Some(reply) =>
        expect("got a ping") {
          inner.log
        }
        assert("pong" === reply)
    }
  }

  @Test
  def test_bangbangbang_GetFutureTimeout2 = {
    val future = server !! OneWay
    future.receiveWithin(100) match {
      case None =>
        expect("got a oneway") {
          inner.log
        }
      case Some(reply) =>
        fail("expected a timeout, got Some(reply)")
    }
  }

  @Test
  def testHotSwap = {
    // using base
    expect("pong") {
      (server !!! Ping).getOrElse("nil")
    }

    // hotswapping
    server.hotswap(Some({
      case Ping => reply("hotswapped pong")
    }))
    expect("hotswapped pong") {
      (server !!! Ping).getOrElse("nil")
    }
  }

  @Test
  def testDoubleHotSwap = {
    // using base
    expect("pong") {
      (server !!! Ping).getOrElse("nil")
    }

    // hotswapping
    server.hotswap(Some({
      case Ping => reply("hotswapped pong")
    }))
    expect("hotswapped pong") {
      (server !!! Ping).getOrElse("nil")
    }

    // hotswapping again
    server.hotswap(Some({
      case Ping => reply("hotswapped pong again")
    }))
    expect("hotswapped pong again") {
      (server !!! Ping).getOrElse("nil")
    }
  }


  @Test
  def testHotSwapReturnToBase = {
    // using base
    expect("pong") {
      (server !!! Ping).getOrElse("nil")
    }

    // hotswapping
    server.hotswap(Some({
      case Ping => reply("hotswapped pong")
    }))
    expect("hotswapped pong") {
      (server !!! Ping).getOrElse("nil")
    }

    // restoring original base
    server.hotswap(None)
    expect("pong") {
      (server !!! Ping).getOrElse("nil")
    }
  }
}


class GenericServerContainerActor extends GenericServer  {
  var log = ""

  override def body: PartialFunction[Any, Unit] = {
    case Ping =>
      log = "got a ping"
      reply("pong")

    case OneWay =>
      log = "got a oneway"
  }

  override def init(config: AnyRef) = log = "initializing: " + config
  override def shutdown(reason: AnyRef) = log = "terminating: " + reason
}


