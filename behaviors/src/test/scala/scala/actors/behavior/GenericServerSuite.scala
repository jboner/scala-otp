/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.behavior

import org.testng.annotations.Test

import org.scalatest.testng.TestNGSuite
import org.scalatest._

import scala.actors.Actor._

class GenericServerSuite extends TestNGSuite {
  @Test 
  def testSendRegularMessage = {
    val server = new TestGenericServerActor
    server.start
    server !? Ping match {
      case reply: String => 
        assert("got a ping" === server.log)
        assert("pong" === reply)
      case _ => fail()
    }
  }
}

class TestGenericServerActor extends GenericServer  {
  var log: String = ""
  
  override def body: PartialFunction[Any, Unit] = {
    case Ping =>
      log = "got a ping"
      reply("pong")
  }
}

