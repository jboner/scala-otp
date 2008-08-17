package scala.actors.controlflow

import org.testng.annotations.{Test, BeforeMethod}

import org.scalatest.testng.TestNGSuite
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._
import org.scalatest._

import scala.actors.controlflow._
import scala.actors.controlflow.ControlFlow._
import scala.actors.controlflow.ControlFlowTestHelper._

/**
 * Tests for control flow.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
class ControlFlowSuite extends TestNGSuite with Checkers {

  @Test
  def testExceptionHandler = {
    asyncTest(10000) {
      val caller = Actor.self
      class TestException extends java.lang.Exception
      try {
        { fc: FC[Unit] => 
          assert(Actor.self != caller) // Should be running in a different actor.
          throw new TestException
        }.toFunction.apply
        fail("Expected Exception to be thrown.")
      } catch {
        case te: TestException => () // Desired result.
        case t: Throwable => fail("Expected TestException, caught: " + t)
      }
    }
  }

  @Test
  def testCallWithCCExceptionHandler = {
    asyncTest(10000) {
      val caller = Actor.self
      class TestException extends java.lang.Exception
      try {
        { fc: FC[Unit] => 
          import fc.implicitThr
          assert(Actor.self != caller) // Should be running in a different actor.
          throw new TestException
        }.toFunction.apply
        fail("Expected Exception to be thrown.")
      } catch {
        case te: TestException => () // Desired result.
        case t: Throwable => fail("Expected TestException, caught: " + t)
      }
    }
  }

  @Test
  def testExceptionHandlerChaining = {
    asyncTest(10000) {
      val caller = Actor.self
      class TestException extends java.lang.Exception
      try {
        { fc: FC[Unit] => 
          import fc.implicitThr
          val asyncFunction: AsyncFunction0[Unit] = { () => }.toAsyncFunction
          asyncFunction { () =>
            assert(Actor.self != caller) // Should be running in a different actor.
            throw new TestException
          }
        }.toFunction.apply
        fail("Expected Exception to be thrown.")
      } catch {
        case te: TestException => () // Desired result.
        case t: Throwable => fail("Expected TestException, caught: " + t)
      }
    }
  }

}
