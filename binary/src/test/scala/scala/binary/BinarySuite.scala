package scala.binary

import org.testng.annotations.{Test, BeforeMethod}

import org.scalatest.testng.TestNGSuite
import org.scalatest.prop.Checkers
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Prop._
import org.scalatest._

/**
 * Tests for Binary objects.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
class BinarySuite extends TestNGSuite with Checkers {

  implicit def arbBinary: Arbitrary[Binary] = Arbitrary {
    for (bytes <- Arbitrary.arbitrary[Array[Byte]]) yield Binary.fromArray(bytes)
  }

  def element(seq: RandomAccessSeq[Byte], i: Int): Option[Byte] = try {
    Some(seq(i))
  } catch {
    case _: IndexOutOfBoundsException => None
  }

  // Workaround for bug #1309.
  def fixRAS(any: Any) = any match {
    case a: Array[Byte] => a
    case ras: RandomAccessSeq[Byte] => ras
  }

  def same(aThunk: => RandomAccessSeq[Byte], bThunk: => RandomAccessSeq[Byte]): Boolean = {
    import java.lang.Exception
    def getResult(thunk: => RandomAccessSeq[Byte]): Either[Class[Exception], RandomAccessSeq[Byte]] = try {
      Right(thunk)
    } catch {
      case e: Exception => {
        val eClass = e.getClass
        //println(eClass)
        Left(eClass.asInstanceOf[Class[Exception]])
      }
    }
    val aResult = getResult(aThunk)
    val bResult = getResult(bThunk)
    if (aResult.isLeft && bResult.isLeft) {
      aResult.left.get == bResult.left.get
    } else if (aResult.isRight && bResult.isRight) {
      //println(aResult.right.get.getClass)
      val a: RandomAccessSeq[Byte] = fixRAS(aResult.right.get)
      val b: RandomAccessSeq[Byte] = fixRAS(bResult.right.get)
      if (a.size != b.size) return false
      for (i <- -5 until (a.size + 5)) { if (element(a, i) != element(b, i)) return false }
      true
    } else {
      false
    }
  }

  @Test
  def testCreate = {
    check((array: Array[Byte]) =>
      same(Binary.fromArray(array), array))
    check((array: Array[Byte]) =>
      same(Binary.fromArray(array), array))
    check { (b0: Byte) =>
      val binary = Binary(b0)
      b0 == binary(0) && binary.length == 1
    }
    check { (b0: Byte, b1: Byte) =>
      val binary = Binary(b0, b1)
      b0 == binary(0) && b1 == binary(1) && binary.length == 2
    }
    check { (b0: Byte, b1: Byte, b2: Byte) =>
      val binary = Binary(b0, b1, b2)
      b0 == binary(0) && b1 == binary(1) && b2 == binary(2) && binary.length == 3
    }
  }

  @Test
  def testCreateWithOffset = {
    check((array: Array[Byte], pre: Array[Byte], post: Array[Byte]) => {
      val joined = pre ++ array ++ post
      Binary.fromArray(joined, pre.length, array.length) == Binary.fromArray(array)
    })
  }

  @Test
  def testToArray = {
    check((array: Array[Byte]) =>
      same(Binary.fromArray(array).toArray, array))
  }

  @Test
  def testEquals = {
    check((binary: Binary) =>
      binary == binary)
    check((binary: Binary) =>
      binary == Binary.fromArray(binary.toArray))
  }

  @Test
  def testHashCode = {
    check((binary: Binary) =>
      binary.hashCode == binary.hashCode)
    check((binary: Binary) =>
      binary.hashCode == Binary.fromArray(binary.toArray).hashCode)
  }

  @Test
  def testImmutable = {
    check((array: Array[Byte]) =>
      (array.length >= 1) ==> {
        val binary = Binary.fromArray(array)
        for (i <- 0 until array.length) array(i) = (array(i) + 1).asInstanceOf[Byte]
        !same(binary, array)
    })
  }

  @Test
  def testAppend = {
    check((array1: Array[Byte], array2: Array[Byte]) =>
        same(array1 ++ array2, Binary.fromArray(array1) ++ Binary.fromArray(array2)))
    check((array1: Array[Byte], array2: Array[Byte]) =>
        same(array1 ++ array2, (Binary.fromArray(array1) ++ Binary.fromArray(array2)).toArray))
    check((array1: Array[Byte], array2: Array[Byte]) =>
        same(array1, (Binary.fromArray(array1) ++ Binary.fromArray(array2)).slice(0, array1.length)))
    check((array1: Array[Byte], array2: Array[Byte]) =>
        same(array2, (Binary.fromArray(array1) ++ Binary.fromArray(array2)).slice(array1.length, array1.length + array2.length)))
    check((array1: Array[Byte], array2: Array[Byte]) =>
        same(array2, (Binary.fromArray(array1) ++ Binary.fromArray(array2)).slice(array1.length).force))
    check { (arrays: List[Array[Byte]]) =>
      val arrayAppend = arrays.foldLeft(new Array[Byte](0)) { (_: Array[Byte]) ++ (_: Array[Byte]) }
      val binaryAppend = arrays.foldLeft(Binary.empty) { (_: Binary) ++ Binary.fromArray((_: Array[Byte])) }
      //println(arrayAppend)
      //println(binaryAppend)
      same(arrayAppend, binaryAppend)
    }
  }
  
  @Test
  def testSlice = {
    val array = new Array[Byte](0)
    same(array.take(5), Binary.fromArray(array).take(5))
    check((array: Array[Byte]) =>
        same(array.take(5), Binary.fromArray(array).take(5)))
  }
  
  @Test
  def testTake = {
    val array = new Array[Byte](0)
    same(array.take(5), Binary.fromArray(array).take(5))
    check((array: Array[Byte]) =>
        same(array.take(5), Binary.fromArray(array).take(5)))
  }

}
