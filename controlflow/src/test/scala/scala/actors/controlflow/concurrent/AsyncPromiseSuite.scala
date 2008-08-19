package scala.actors.controlflow.concurrent

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
 * Tests <code>AsyncPromise</code>.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
class AsyncPromiseSuite extends TestNGSuite with Checkers {

  @Test
  def testFuture = {
    asyncTest(10000) {
      implicit val implicitThr = thrower
      val p = new AsyncPromise[String]
      println("Checking if promise is set.")
      assert(!p.isSet)
      println("Setting promise.")
      p.set(Return("Hello"))
      println("Checking if promise is set.")
      assert(p.isSet)
    }
  }
}
