package scala.actors.controlflow

import org.testng.annotations.{Test, BeforeMethod}

import org.scalatest.testng.TestNGSuite
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._
import org.scalatest._

import scala.actors.controlflow.ControlFlow._

/**
 * Tests for control flow.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
class ControlFlowSuite extends TestNGSuite with Checkers {

  // XXX: Make shared function.
  private[this] def asyncTest(msec: Long)(body: => Unit) =
    callWithCCWithin(msec) (asAsync(() => body))

  @Test
  def testExceptionHandler = {
    asyncTest(1000) {
      val caller = Actor.self
      class TestException extends java.lang.Exception
      try {
        callWithCC { k: Cont[Unit] => 
          assert(Actor.self != caller) // Should be running in a different actor.
          throw new TestException
        }
        fail("Expected Exception to be thrown.")
      } catch {
        case te: TestException => () // Desired result.
        case t: Throwable => fail("Expected TestException, caught: " + t)
      }
    }
  }

  @Test
  def testCallWithCCExceptionHandler = {
    asyncTest(1000) {
      val caller = Actor.self
      class TestException extends java.lang.Exception
      try {
        callWithCC { k: Cont[Unit] => 
          import k.exceptionHandler
          assert(Actor.self != caller) // Should be running in a different actor.
          throw new TestException
        }
        fail("Expected Exception to be thrown.")
      } catch {
        case te: TestException => () // Desired result.
        case t: Throwable => fail("Expected TestException, caught: " + t)
      }
    }
  }

  @Test
  def testExceptionHandlerChaining = {
    asyncTest(1000) {
      val caller = Actor.self
      class TestException extends java.lang.Exception
      try {
        callWithCC { k: Cont[Unit] => 
          import k.exceptionHandler
          val asyncFunction: AsyncFunction0[Unit] = asAsync { () => }
          asyncFunction { () =>
            assert(Actor.self != caller) // Should be running in a different actor.
            throw new TestException
          }
        }
        fail("Expected Exception to be thrown.")
      } catch {
        case te: TestException => () // Desired result.
        case t: Throwable => fail("Expected TestException, caught: " + t)
      }
    }
  }

}
