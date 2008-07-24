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

  def range(from: Int, until: Int, delay: Long) = {
    def tailFrom(i: Int): AsyncStream[Int] = {
      if (i < until) {
        Thread.sleep(delay)
        AsyncStream.cons(i, asAsync { () => tailFrom(i+1) })
      } else {
        AsyncStream.empty
      }
    }
    tailFrom(from)
  }

  @Test
  def testAsyncStream = {
    val first = range(0, 2, 50)
    val second = range(2, 4, 50)
    assert(List(0, 1) == callWithCC(first.toList _))
    assert(List(0, 1, 2, 3) == callWithCC { k: Cont[List[Int]] =>
      import k.exceptionHandler
      first.append(asAsync { () => second }) { combined: AsyncStream[Int] =>
        combined.toList(k)
      }
    })
  }

  @Test
  def testAsyncFunction = {
    val addOne = asAsync { x: Int => x + 1 }
    val double = asAsync { x: Int => x * 2 }
    assert(callWithCC(addOne(2) _) == 3)
    assert(callWithCC(double(2) _) == 4)
    assert(callWithCC((addOne andThen double)(2) _) == 6)
    assert(callWithCC((addOne andThen double)(2) _) == 6)
    assert(callWithCC((double andThen addOne)(2) _) == 5)
    assert(callWithCC((addOne compose double)(2) _) == 5)
    assert(callWithCC((double compose addOne)(2) _) == 6)

    val two = asAsync { () => 2 }
    assert(callWithCC(two) == 2)
    assert(callWithCC((two andThen addOne)) == 3)
    assert(callWithCC((two andThen double)) == 4)
    assert(callWithCC((two andThen addOne andThen double)) == 6)
    assert(callWithCC((two andThen addOne andThen double)) == 6)
    assert(callWithCC((two andThen double andThen addOne)) == 5)
    assert(callWithCC((two andThen (addOne compose double))) == 5)
    assert(callWithCC((two andThen (double compose addOne))) == 6)
  }
  
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
