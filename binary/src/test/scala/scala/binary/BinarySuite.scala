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
    for (bytes <- Arbitrary.arbitrary[Array[Byte]]) yield Binary.fromSeq(bytes)
  }

  def element(seq: RandomAccessSeq[Byte], i: Int): Option[Byte] = try {
    Some(seq(i))
  } catch {
    case _: IndexOutOfBoundsException => None
  }

  def sameBytes(a: RandomAccessSeq[Byte], b: RandomAccessSeq[Byte]): Boolean = {
    if (a.size != b.size) return false
    for (i <- -5 until (a.size + 5)) { if (element(a, i) != element(b, i)) return false }
    true
  }

  def checkClassForLength(length: Int, binary: Binary) = {
    if (length ==0) {
      binary eq Binary0
    } else if (length <= 8) {
      binary.getClass == Class.forName("scala.binary.Binary"+length)
    } else {
      binary.getClass == Class.forName("scala.binary.ArrayBinary")
    }
  }

  @Test
  def testCreate = {
    check((array: Array[Byte]) =>
      checkClassForLength(array.length, Binary.fromSeq(array)))
    check((array: Array[Byte]) =>
      sameBytes(Binary.fromSeq(array), array))
    check((array: Array[Byte]) =>
      sameBytes(Binary.fromSeq(array), array))
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
      Binary.fromSeq(joined, pre.length, array.length) == Binary.fromSeq(array)
    })
  }

  @Test
  def testToArray = {
    check((array: Array[Byte]) =>
      sameBytes(Binary.fromSeq(array).toArray, array))
  }

  @Test
  def testEquals = {
    check((binary: Binary) =>
      binary == binary)
    check((binary: Binary) =>
      binary == Binary.fromSeq(binary.toArray))
  }

  @Test
  def testHashCode = {
    check((binary: Binary) =>
      binary.hashCode == binary.hashCode)
    check((binary: Binary) =>
      binary.hashCode == Binary.fromSeq(binary.toArray).hashCode)
  }

  @Test
  def testImmutable = {
    check((array: Array[Byte]) =>
      (array.length >= 1) ==> {
        val binary = Binary.fromSeq(array)
        for (i <- 0 until array.length) array(i) = (array(i) + 1).asInstanceOf[Byte]
        !sameBytes(binary, array)
    })
  }


  @Test
  def testAppend = {
    check((array1: Array[Byte], array2: Array[Byte]) =>
        sameBytes(array1 ++ array2, Binary.fromSeq(array1) ++ Binary.fromSeq(array2)))
    check((array1: Array[Byte], array2: Array[Byte]) =>
        sameBytes(array1 ++ array2, (Binary.fromSeq(array1) ++ Binary.fromSeq(array2)).toArray))
    check((array1: Array[Byte], array2: Array[Byte]) =>
        sameBytes(array1, (Binary.fromSeq(array1) ++ Binary.fromSeq(array2)).slice(0, array1.length)))
    check((array1: Array[Byte], array2: Array[Byte]) =>
        sameBytes(array2, (Binary.fromSeq(array1) ++ Binary.fromSeq(array2)).slice(array1.length, array1.length + array2.length)))
  }

}
