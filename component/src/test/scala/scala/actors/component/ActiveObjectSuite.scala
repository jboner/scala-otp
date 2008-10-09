/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.component

import net.liftweb.util.{Can, Full, Empty, Failure}

import org.testng.annotations.{Test, BeforeMethod}

import scala.actors.behavior._
import scala.actors.annotation.oneway

import org.scalatest.testng.TestNGSuite
import org.scalatest._

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class ActiveObjectSuite extends TestNGSuite {

  var messageLog = ""

  trait Foo {
    def foo(msg: String)    
    @oneway def bar(msg: String)
  }

  class FooImpl extends Foo {
    val bar: Bar = new BarImpl 
    def foo(msg: String) = messageLog += msg
    def bar(msg: String) = bar.bar(msg)
  }

  trait Bar {
    def bar(msg: String)
  }

  class BarImpl extends Bar {
    def bar(msg: String) = { 
      Thread.sleep(10)
      messageLog += msg
    }
  }

  @BeforeMethod
  def setup = messageLog = ""

  @Test { val groups=Array("unit") }
  def testCreateGenericServerBasedComponentUsingCustomSupervisor = {
    val proxy = new ActiveObjectProxy(new FooImpl, 100)

    ActiveObject.start(
      RestartStrategy(AllForOne, 3, 100),
      Worker(
        proxy.server,
        LifeCycle(Permanent, 100))
      :: Nil)

    val foo = ActiveObject.newInstance[Foo](classOf[Foo], proxy)

    foo.foo("foo ")
    foo.bar("bar ")
    messageLog += "before bar "

    Thread.sleep(100)
    assert("foo before bar bar " === messageLog)
  }

  @Test { val groups=Array("unit") }
  def testCreateGenericServerBasedComponentUsingDefaultSupervisor = {
    val foo = ActiveObject.newInstance[Foo](classOf[Foo], new FooImpl, 100)

    foo.foo("foo ")
    foo.bar("bar ")
    messageLog += "before bar "

    Thread.sleep(100)
    assert("foo before bar bar " === messageLog)
  }
}
