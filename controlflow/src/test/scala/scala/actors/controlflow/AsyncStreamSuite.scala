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
 * Tests for AsyncStream.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
class AsyncStreamSuite extends TestNGSuite with Checkers {

  def delayedRange(from: Int, until: Int, delay: Long) = {
    def tailFrom(i: Int): AsyncStream[Int] = {
      if (i < until) {
        Thread.sleep(delay)
        AsyncStream.cons(i, { () => tailFrom(i+1) }.toAsyncFunction)
      } else {
        AsyncStream.empty
      }
    }
    tailFrom(from)
  }

  val zeroAndOne = delayedRange(0, 2, 1)
  val threeAndFour = delayedRange(2, 4, 1)
  val zeroToFive = delayedRange(0, 5, 1)

  @Test
  def testToList = {
    asyncTest(10000) {
      check { (list: List[Int]) =>
        //println("testToList: " + list)
        list == AsyncStream.fromList(list).toList
      }
    }
  }

  @Test
  def testAsyncToList = {
    asyncTest(10000) {
      check { (list: List[Int]) =>
        //println("testAsyncToList: " + list)
        list == { fc: FC[List[Int]] => AsyncStream.fromList(list).asyncToList(fc) }.within(1000).toFunction.apply
      }
    }
  }

  @Test
  def testAsyncAppend = {
    asyncTest(10000) {
      println("testAsyncAppend")
      check { (list1: List[Int], list2: List[Int]) =>
        val as1 = AsyncStream.fromList(list1)
        val as2 = AsyncStream.fromList(list2)
        (list1 ++ list2) == { fc: FC[AsyncStream[Int]] => as1.asyncAppend(Return(as2).toAsyncFunction)(fc) }.toFunction.apply.toList
      }
    }
  }

  @Test
  def testIterator = {
    asyncTest(10000) {
      println("testIterator")
      check { (list: List[Int]) =>
        //println("testIterator: " + list)
        val as = AsyncStream.fromList(list)
        list == { fc: FC[AsyncStream[Int]] =>
          AsyncStream.fromAsyncIterator(as.asyncElements)(fc)
        }.within(1000).toFunction.apply.toList
      }
    }
  }

  @Test
  def testAsyncMap = {
    asyncTest(10000) {
      println("testAsyncMap")
      check { (list: List[Int]) =>
        //println("testAsyncMap: " + list)
        def double(x: Int) = x * 2
        val asyncDouble = (double _).toAsyncFunction
        val as = AsyncStream.fromList(list)
        list.map(double) == { fc: FC[AsyncStream[Int]] => as.asyncMap(asyncDouble)(fc) }.within(1000).toFunction.apply.toList
      }
    }
  }

  @Test
  def testMap = {
    asyncTest(10000) {
      println("testMap")
      check { (list: List[Int]) =>
        def double(x: Int) = x * 2
        list.map(double) == AsyncStream.fromList(list).map(double).toList
      }
    }
  }

}
