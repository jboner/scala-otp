package scala.binary

import java.nio.ByteBuffer

/**
 * Useful methods for working Binary objects, including Binary
 * creation.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
object Binary {

  /**
   * Gets a Binary containing no bytes.
   */
  def empty: Binary = new Binary {

    override def length = 0

    override private[binary] def depth = 0

    override def apply(i: Int) = throw new IndexOutOfBoundsException

    protected def copyToArray0(from: Int, until: Int, dest: Array[Byte], destFrom: Int): Unit = ()

    protected def arrays0(from: Int, until: Int): Iterable[ArrayBinary] = Nil

    protected def slice0(from: Int, until: Int): Binary = this

  }

  /**
   * Creates a Binary containing the given bytes.
   */
  def apply(bytes: Byte*): Binary = fromArray(bytes.toArray, false)

  def fromSeq(seq: Seq[Byte]): Binary = seq match {
    case binary: Binary => binary
    case array: Array[Byte] => fromArray(array, true)
    case _ => fromArray(seq.toArray, false)
  }

  def fromArray(array: Array[Byte]): Binary = fromArray(array, true)
  def fromArray(array: Array[Byte], offset: Int, length: Int): Binary = fromArray(array, offset, length, true)
  private[scala] def fromArray(array: Array[Byte], aliased: Boolean): Binary = fromArray(array, 0, array.length, aliased)
  private[scala] def fromArray(array: Array[Byte], offset: Int, length: Int, aliased: Boolean): Binary = {
    if (aliased) {
      val copy = new Array[Byte](length)
      Array.copy(array, offset, copy, 0, length)
      new ArrayBinary(copy, 0, length)
    } else {
      new ArrayBinary(array, offset, length)
    }
  }

  /**
   * Creates a Binary from a String.
   */
  def fromString(string: String): Binary = {
    val bytes = string.getBytes
    fromArray(bytes, false)
  }

  /**
   * Creates a Binary from a String.
   */
  def fromString(string: String, charsetName: String): Binary = {
    val bytes = string.getBytes(charsetName)
    fromArray(bytes, false)
  }

}

/**
 * An immutable, randomly-accessible sequence of bytes.
 */
trait Binary extends RandomAccessSeq[Byte] {

  /**
   * The length of the Binary in bytes.
   */
  def length: Int

  /**
   * The size of the Binary in bytes.
   */
  override def size = length

  private[binary] def depth: Int

  // XXX: Safe to leave unsynchronized? Can a thread ever see a partially-written value?
  private var cachedHashCode = 0

  // same algorithm as java.lang.String, except starting with 4321
  //  4321 + s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]
  override final def hashCode: Int = {
    if (cachedHashCode == 0) {
      var hashCode = 4321
      var i = 0
      while (i < length) {
        hashCode = hashCode * 31 + this(i)
        i += 1
      }
      cachedHashCode = hashCode
    }
    cachedHashCode
  }

  /**
   * Checks if another object is equal to this object. They will be
   * equal if and only if the other object is a Binary containing the
   * same bytes in the same order.
   */
  override final def equals(o: Any): Boolean = {
    if (this eq o.asInstanceOf[AnyRef]) return true
    if (!(this.isInstanceOf[Binary])) return false
    val ob = o.asInstanceOf[Binary]
    if (length != ob.length) return false
    var i = 0
    while (i < length) {
      if (this(i) != ob(i)) return false
      i += 1
    }
    true
  }  

  private def combine(left: Binary, right: Binary): Binary = {
    val combinedArray = (new CompositeBinary(left, right)).toArray
    Binary.fromArray(combinedArray, false)
  }

  private def balance(left: Binary, right: Binary): Binary = {
    val composedArray = new CompositeBinary(left, right)
    if (composedArray.isBalanced) composedArray
    else composedArray.rebalance
  }

  /**
   * Append another Binary to this Binary, returning a new Binary as
   * the result.
   */
  final def ++(other: Binary): Binary = {
    if (isEmpty) {
      other
    } else if (other.isEmpty) {
      this
    } else {
      val newLength = length.asInstanceOf[Long] + other.length.asInstanceOf[Long]
      if (newLength > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Combined length too long: " + newLength)
      }
      val maxCombineLength = 16
      (this, other) match {
        case (l: Binary, r @ CompositeBinary(rl, rr)) if (l.length + rl.length) <= maxCombineLength => {
          balance(combine(l, rl), rr)
        }
        case (l @ CompositeBinary(ll, lr), r: Binary) if (lr.length + r.length) <= maxCombineLength => {
          balance(ll, combine(lr, r))
        }
        case (l: Binary, r: Binary) => {
          balance(l, r)
        }
      }
    }
  }

  /**
   * Gets a slice of this binary, returning a new Binary as the
   * result.
   */
  override final def slice(from: Int, until: Int): Binary = {
    if (from == 0 && until == length) this
    else if (from < 0 || until > length) throw new IndexOutOfBoundsException
    else if (from > until) throw new IllegalArgumentException("Argument 'from' was > 'until'.")
    else slice0(from, until)
  }

  protected def slice0(from: Int, until: Int): Binary

  /**
   * Gets a slice of this binary, returning a new Binary as the
   * result.
   */
  override final def slice(from: Int) = slice(from, length)


  /**
   * Copy this object's bytes into a given array.
   */
  final def copyToArray(from: Int, until: Int, dest: Array[Byte], destFrom: Int): Unit = {
    if (from < 0 || until > length) throw new IndexOutOfBoundsException
    else if ((until - from) > (dest.length - destFrom)) throw new IndexOutOfBoundsException
    else if (from == until) ()
    else copyToArray0(from, until, dest, destFrom)
  }

  protected def copyToArray0(from: Int, until: Int, dest: Array[Byte], destFrom: Int): Unit

  /**
   * Get a copy of this object's bytes, stored in an array.
   */
  def toArray: Array[Byte] = {
    val array = new Array[Byte](length)
    copyToArray(0, length, array, 0)
    array
  }

  /**
   * Gets the ArrayBinary leaves of this Binary.
   */
  private[scala] final def arrays(from: Int, until: Int): Iterable[ArrayBinary] = {
    if (from < 0 || until > length) throw new IndexOutOfBoundsException
    else if (from == until) Nil
    else arrays0(from, until)
  }

  protected def arrays0(from: Int, until: Int): Iterable[ArrayBinary]

  /**
   * UNSAFE: Get a list of ByteBuffers containing this object's
   * content. It is important not to modify the content of any buffer,
   * as this will alter the content of this Binary - which must not
   * happen.
   */
  private[scala] final def byteBuffers(from: Int, until: Int): Iterable[ByteBuffer] =
    arrays0(from, until) map { _.wrappingByteBuffer }

 private[scala] final def byteBuffers: Iterable[ByteBuffer] =
    byteBuffers(0, length)

  /**
   * Get a textual representation of this object.
   */
  override def toString = toHexString

  /**
   * Get a hexadecimal representation of this object's bytes.
   */
  def toHexString: String = {
    val builder = new StringBuilder(length * 2)
    for (byte <- this) {
      val unsigned = (256 + byte) % 256
      val upper = unsigned >> 4
      val lower = unsigned & 0xf
      builder.append(Character.forDigit(upper, 16))
      builder.append(Character.forDigit(lower, 16))
    }
    builder.toString
  }

  /**
   * Gets a big-endian-encoded Long from the given index.
   */
  def getBELong(index: Int): Long = {
    this(index+0).asInstanceOf[Long] << 56 |
    this(index+1).asInstanceOf[Long] << 48 |
    this(index+2).asInstanceOf[Long] << 40 |
    this(index+3).asInstanceOf[Long] << 32 |
    this(index+4).asInstanceOf[Long] << 24 |
    this(index+5).asInstanceOf[Long] << 16 |
    this(index+6).asInstanceOf[Long] << 8 |
    this(index+7).asInstanceOf[Long] << 0
  }

  // TODO: Write more conversion functions...

}
