package scala.binary

import java.io.ObjectInput
import java.io.ObjectOutput
import java.io.Serializable
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset._

/**
 * Useful methods for working Binary objects, including Binary
 * creation.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
object Binary {

  /**
   * Gets an empty Binary.
   */
  def empty: Binary = EmptyBinary

  /**
   * Creates a Binary containing the given bytes.
   */
  def apply(bytes: Byte*): Binary = fromArray(bytes.toArray, false)

  /**
   * Creates a Binary from a copy of the given sequence.
   */
  def fromSeq(seq: Seq[Byte]): Binary = seq match {
    case binary: Binary => binary
    case array: Array[Byte] => fromArray(array, true)
    case _ => fromArray(seq.toArray, false)
  }

  /**
   * Creats a Binary from a copy of the given bytes.
   */
  def fromArray(array: Array[Byte]): Binary = fromArray(array, true)
  
  /**
   * Creates a Binary from a copy of a slice of the given bytes.
   */
  def fromArray(array: Array[Byte], offset: Int, length: Int): Binary =
    fromArray(array, offset, length, true)
  
  /**
   * UNSAFE: Creates a Binary from the given bytes, optionally copying them.
   *
   * <p>This method exposes internal implementation details, allowing callers
   * to violate the immutability of Binary objects. Nevertheless, it is made
   * available to the 'scala' package to permit certain optimisations.
   */
  private[scala] def fromArray(array: Array[Byte], aliased: Boolean): Binary =
    fromArray(array, 0, array.length, aliased)

  /**
   * UNSAFE: Creates a Binary from the given bytes, optionally copying them.
   *
   * <p>This method exposes internal implementation details, allowing callers
   * to violate the immutability of Binary objects. Nevertheless, it is made
   * available to the 'scala' package to permit certain optimisations.
   *
   * @param array The bytes to create the Binary from.
   * @param offset The first byte to include in the Binary.
   * @param length The length of the Binary.
   * @param aliased Whether or not the bytes may be aliased outside this Binary.
   * If <code>true</code>, then a copy of the bytes will be taken to create the
   * Binary. If <code>false</code>, then the given bytes may be used to create
   * an ArrayBinary.
   */
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
   * Creates a Binary from a String, using the platform's default character
   * encoding.
   */
  def fromString(string: String): Binary = {
    val bytes = string.getBytes
    fromArray(bytes, false)
  }

  /**
   * Creates a Binary from a String, using the given character encoding.
   */
  def fromString(string: String, charsetName: String): Binary = {
    val bytes = string.getBytes(charsetName)
    fromArray(bytes, false)
  }

  trait BinaryLike extends RandomAccessSeq[Byte] {
    def force: Binary
  }
  trait Projection extends RandomAccessSeq.Projection[Byte] with BinaryLike {
    override def force = fromSeq(this)
    override def drop( from: Int) = slice(from, length)
    override def take(until: Int) = slice(0, until)
    override def dropWhile(p: Byte => Boolean) = {
      val c = length + 1
      drop((findIndexOf(!p(_)) + c) % c)
    }
    override def takeWhile(p: Byte => Boolean) = {
      val c = length + 1
      take((findIndexOf(!p(_)) + c) % c)
    }
    private[binary] def forcedSlice(from0: Int, until0: Int) =
      slice(from0, until0).force
    override def slice(from0: Int, until0: Int): Projection = new RandomAccessSeq.Slice[Byte] with Projection {
      override def from = from0
      override def until = until0
      override def underlying = Projection.this
      override def slice(from0: Int, until0: Int) =
        Projection.this.slice(from + from0, from + until0)
      override def force = underlying.forcedSlice(from, until)
    }
    override def slice(from0: Int) = slice(from0, length)
    override def reverse: Projection = new Projection {
      def apply(i: Int) = Projection.this.apply(length - i - 1)
      def length = Projection.this.length
    }
  }

}

/**
 * An immutable, randomly-accessible sequence of bytes. The current
 * implementation is a concatenation tree, or "rope". Compared to a simple
 * <code>Array</code> of bytes, this data structure emphasises the performance
 * of concatenation and slice operations over that of random access. Iteration
 * is still fast.
 *
 * @see http://www.cs.ubc.ca/local/reading/proceedings/spe91-95/spe/vol25/issue12/spe986.pdf
 */
@serializable
trait Binary extends RandomAccessSeq[Byte] with Binary.BinaryLike with Serializable {

  /**
   * The length of the Binary in bytes.
   */
  def length: Int

  /**
   * The size of the Binary in bytes.
   */
  override def size = length

  /**
   * The depth of this Binary's tree, where leaves have depth 0.
   */
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
  
  override def projection: Binary.Projection = new Binary.Projection {
    def length = Binary.this.length
    def apply(i: Int) = Binary.this.apply(i)
    override def force = Binary.this
    override def forcedSlice(from0: Int, until0: Int) =
      Binary.this.forcedSlice(from0, until0)
  }
  override def slice(from: Int, until: Int): Binary.Projection = projection.slice(from, until)
  override def slice(from: Int): Binary.Projection = projection.slice(from)
  override def take(until: Int): Binary.Projection = projection.take(until)
  override def drop(from: Int): Binary.Projection = projection.drop(from)
  override def dropWhile(p: Byte => Boolean) = projection.dropWhile(p)
  override def takeWhile(p: Byte => Boolean) = projection.takeWhile(p)
  override def reverse = projection.reverse
  override def force = this

  /**
   * Combine two Binaries into a single ArrayBinary. Used by ++.
   */
  private def combine(left: Binary, right: Binary): Binary = {
    val combinedArray = (new CompositeBinary(left, right)).toArray
    Binary.fromArray(combinedArray, false)
  }

  /**
   * Combine to Binaries into a single, balanced CompositeBinary. Used by ++.
   */
  private def balance(left: Binary, right: Binary): Binary = {
    val composedArray = new CompositeBinary(left, right)
    if (composedArray.isBalanced) composedArray
    else composedArray.rebalance
  }

  /**
   * Append another Binary to this Binary, returning the resulting aggregation.
   *
   * @param other The Binary to append.
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
  private[binary] final def forcedSlice(from: Int, until: Int): Binary = {
    if (from == 0 && until == length) this
    else if (from < 0 || until > length) throw new IndexOutOfBoundsException
    else if (from > until) throw new IllegalArgumentException("Argument 'from' was > 'until'.")
    else forcedSlice0(from, until)
  }

  /**
   * Gets a slice of this binary, returning a new Binary as the
   * result.
   */
  private[binary] final def forcedSlice(from: Int) = slice(from, length)

  /**
   * Internal implementation of slice operation. Called by slice after bounds
   * checking and some simple optimisations have been performed.
   */
  protected def forcedSlice0(from: Int, until: Int): Binary

  /**
   * Copy this object's bytes into a given array.
   */
  final def copyToArray(from: Int, until: Int, dest: Array[Byte], destFrom: Int): Unit = {
    if (from < 0 || until > length) throw new IndexOutOfBoundsException
    else if ((until - from) > (dest.length - destFrom)) throw new IndexOutOfBoundsException
    else if (from == until) ()
    else copyToArray0(from, until, dest, destFrom)
  }

  /**
   * Internal implementation of copyToArray operation. Called by copyToArray
   * after bounds checking and some simple optimisations have been performed.
   */
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
   * Gets the ArrayBinary leaves of this Binary in a given range.
   *
   * <p>This method exposes internal implementation details of Binary objects,
   * and so is only made available to the 'scala' package, in order to permit
   * certain optimisations.
   */
  private[scala] final def arrays(from: Int, until: Int): Iterable[ArrayBinary] = {
    if (from < 0 || until > length) throw new IndexOutOfBoundsException
    else if (from == until) Nil
    else arrays0(from, until)
  }

  /**
   * Gets all the ArrayBinary leaves of this Binary.
   *
   * <p>This method exposes internal implementation details of Binary objects,
   * and so is only made available to the 'scala' package, in order to permit
   * certain optimisations.
   */
  private[scala] final def arrays: Iterable[ArrayBinary] =
    arrays(0, length)

  /**
   * Internal implementation of arrays operation. Called by arrays
   * after bounds checking and some simple optimisations have been performed.
   */
  protected def arrays0(from: Int, until: Int): Iterable[ArrayBinary]

  /**
   * UNSAFE: Get a list of ByteBuffers containing this object's
   * content. It is important not to modify the content of any buffer,
   * as this will alter the content of this Binary - which must not
   * happen.
   *
   * <p>This method exposes internal implementation details, allowing callers
   * to violate the immutability of Binary objects. Nevertheless, it is made
   * available to the 'scala' package to permit certain optimisations.
   */
  private[scala] final def byteBuffers(from: Int, until: Int): Iterable[ByteBuffer] =
    arrays0(from, until) map { _.wrappingByteBuffer }

  private[scala] final def byteBuffers: Iterable[ByteBuffer] =
    byteBuffers(0, length)

  /**
   * Get a textual representation of this object.
   */
  override def toString = {
    val builder = new StringBuilder()
    builder.append("Binary(")
    val maxLength = 16
    take(maxLength).addString(builder, ", ")
    if (length > maxLength) {
      val remaining = length - maxLength
      builder.append(", [")
      builder.append(remaining)
      builder.append(" more]")
    }
    builder.append(")")
    builder.toString
  }

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
   * Decodes the Binary into a String, using a given charset.
   *
   * @throws java.nio.BufferUnderflowException If the Binary contains too few bytes.
   * @throws java.nio.charset.MalformedInputException If the input is malformed.
   * @throws java.nio.charset.UnmappableCharacterException If the output cannot be represented in the charset.
   */
  def decodeString(charsetName: String): String = {
    val charset = Charset.forName(charsetName)
    val decoder = charset.newDecoder()
    val maxChars = (decoder.maxCharsPerByte().asInstanceOf[Double] * length).asInstanceOf[Int]
    val charArray = new Array[Char](maxChars)
    val charBuffer = CharBuffer.wrap(charArray)
    val result = decodeStringWith(decoder, charBuffer)
    result match {
      case None => {
        decoder.decode(ByteBuffer.allocate(0), charBuffer, true)
        decoder.flush(charBuffer)
        new String(charArray, 0, charBuffer.position)
      }
      case Some(coderResult) => {
        coderResult.throwException
        throw new AssertionError("Unreachable code")
      }
    }
  }

  /**
   * Decodes the Binary into a String using the given decoder and output buffer.
   *
   * @return None if decoding succeeds, or Some(coderResult) with the
   * error causing failure.
   */
  def decodeStringWith(decoder: CharsetDecoder, charBuffer: CharBuffer): Option[CoderResult] = {
    for (bb <- byteBuffers) {
      val result = decoder.decode(bb, charBuffer, false)
      if (result.isError) return Some(result)
    }
    None
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
