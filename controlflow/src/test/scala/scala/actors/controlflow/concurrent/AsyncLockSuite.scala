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
 * Tests <code>AsyncLock</code>.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
class AsyncLockSuite extends TestNGSuite with Checkers {

  @Test
  def testSingle = {
    asyncTest(10000) {
      implicit val implicitThr = thrower
      val af: AsyncFunction0[Unit] = { (fc: FC[Unit]) =>
        println("Creating lock.")
        val l = new AsyncLock
        println("Acquiring lock.")
        l.lock { () =>
          println("Releasing lock.")
          l.unlock
          fc.ret(())
        }
      }
      af.toFunction.apply
    }
  }

  @Test
  def testParallel = {
    asyncTest(10000) {
      // run 100 actors, locking and unlocking 1000 times or for 5 seconds
      // for each, collect the time post-lock and pre-unlock times
      // after the run, aggregate the results and check there are no overlaps
      implicit val implicitThr = thrower
      val af: AsyncFunction0[Unit] = { (fc: FC[Unit]) =>
        println("Creating lock.")
        val l = new AsyncLock
        println("Acquiring lock.")
        l.lock { () =>
          println("Releasing lock.")
          l.unlock
          fc.ret(())
        }
      }
      af.toFunction.apply
    }
  }
}
