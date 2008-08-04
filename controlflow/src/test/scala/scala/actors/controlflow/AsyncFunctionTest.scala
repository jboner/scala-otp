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
class AsyncFunctionSuite extends TestNGSuite with Checkers {

  // XXX: Make shared function.
  private[this] def asyncTest(msec: Long)(body: => Unit) =
    callWithCCWithin(msec) (asAsync(() => body))

  val addOne = asAsync { x: Int => x + 1 }
  val double = asAsync { x: Int => x * 2 }
  val two = asAsync { () => 2 }

  @Test
  def testAsyncFunction = {
    assert(callWithCC(addOne(2) _) == 3)
    assert(callWithCC(double(2) _) == 4)
    assert(callWithCC((addOne andThen double)(2) _) == 6)
    assert(callWithCC((addOne andThen double)(2) _) == 6)
    assert(callWithCC((double andThen addOne)(2) _) == 5)
    assert(callWithCC((addOne compose double)(2) _) == 5)
    assert(callWithCC((double compose addOne)(2) _) == 6)

    assert(callWithCC(two) == 2)
    assert(callWithCC((two andThen addOne)) == 3)
    assert(callWithCC((two andThen double)) == 4)
    assert(callWithCC((two andThen addOne andThen double)) == 6)
    assert(callWithCC((two andThen addOne andThen double)) == 6)
    assert(callWithCC((two andThen double andThen addOne)) == 5)
    assert(callWithCC((two andThen (addOne compose double))) == 5)
    assert(callWithCC((two andThen (double compose addOne))) == 6)
  }
  
}
