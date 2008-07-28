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
 * Tests for AsyncStream.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
class AsyncStreamSuite extends TestNGSuite with Checkers {
  
  // XXX: Make shared function.
  private[this] def asyncTest(msec: Long)(body: => Unit) =
    callWithCCWithin(msec) (asAsync(() => body))

  def delayedRange(from: Int, until: Int, delay: Long) = {
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

  val zeroAndOne = delayedRange(0, 2, 10)
  val threeAndFour = delayedRange(2, 4, 10)
  val zeroToFive = delayedRange(0, 5, 10)

  @Test
  def testToList = {
    asyncTest(1000) {
      check { (list: List[Int]) =>
        list == AsyncStream.fromList(list).toList
      }
      check { (list: List[Int]) =>
        list == callWithCC { k: Cont[List[Int]] => AsyncStream.fromList(list).asyncToList(k) }
      }
    }
  }

  @Test
  def testAsyncAppend = {
    asyncTest(1000) {
      check { (list1: List[Int], list2: List[Int]) =>
        val as1 = AsyncStream.fromList(list1)
        val as2 = AsyncStream.fromList(list2)
        (list1 ++ list2) == (callWithCC { k: Cont[AsyncStream[Int]] => as1.asyncAppend(asAsync(() => as2))(k) }).toList
      }
    }
  }

  @Test
  def testAsyncMap = {
    asyncTest(1000) {
      check { (list: List[Int]) =>
        def double(x: Int) = x * 2
        val asyncDouble = asAsync(double _)
        val as = AsyncStream.fromList(list)
        list.map(double) == (callWithCC { k: Cont[AsyncStream[Int]] => as.asyncMap(asyncDouble)(k) }).toList
      }
    }
  }

  @Test
  def testMap = {
    asyncTest(1000) {
      check { (list: List[Int]) =>
        def double(x: Int) = x * 2
        list.map(double) == AsyncStream.fromList(list).map(double).toList
      }
    }
  }

}
