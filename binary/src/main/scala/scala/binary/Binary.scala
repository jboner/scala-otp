package scala.binary

/**
 * Useful methods for working Binary objects, including Binary
 * creation.
 *
 * @author <a href="http://www.richdougherty.com/">Rich Dougherty</a>
 */
object Binary {

  /**
   * Creates a Binary containing a copy of the given bytes.
   */
  def apply(bytes: Array[Byte]): Binary = this(bytes, 0, bytes.length)

  /**
   * Creates a Binary containing a copy of the given bytes in the
   * given range.
   */
  def apply(bytes: Array[Byte], offset: Int, length: Int): Binary = {
    val small = makeSmallCopy(bytes, offset, length)
    if (small.isDefined) return small.get
    val copy = new Array[Byte](length)
    Array.copy(bytes, offset, copy, 0, length)
    new ArrayBinary(copy)
  }

  /**
   * Attempt to create a small Binary from the given bytes. This
   * operation may fail (return None), in which case the Binary will
   * have to be created with an alternative method.
   */
  private def makeSmallCopy(bytes: Array[Byte], offset: Int, length: Int): Option[Binary] =
    length match {
      case 0 => Some(new Binary0)
      case 1 => Some(new Binary1(
        bytes(offset)
      ))
      case 2 => Some(new Binary2(
        bytes(offset),
        bytes(offset+1)
      ))
      case 3 => Some(new Binary3(
        bytes(offset),
        bytes(offset+1),
        bytes(offset+2)
      ))
      case 4 => Some(new Binary4(
        bytes(offset),
        bytes(offset+1),
        bytes(offset+2),
        bytes(offset+3)
      ))
      case 5 => Some(new Binary5(
        bytes(offset),
        bytes(offset+1),
        bytes(offset+2),
        bytes(offset+3),
        bytes(offset+4)
      ))
      case 6 => Some(new Binary6(
        bytes(offset),
        bytes(offset+1),
        bytes(offset+2),
        bytes(offset+3),
        bytes(offset+4),
        bytes(offset+5)
      ))
      case 7 => Some(new Binary7(
        bytes(offset),
        bytes(offset+1),
        bytes(offset+2),
        bytes(offset+3),
        bytes(offset+4),
        bytes(offset+5),
        bytes(offset+6)
      ))
      case 8 => Some(new Binary8(
        bytes(offset),
        bytes(offset+1),
        bytes(offset+2),
        bytes(offset+3),
        bytes(offset+4),
        bytes(offset+5),
        bytes(offset+6),
        bytes(offset+7)
      ))
      case _ => None
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

  override def hashCode: Int = {
    // same algorithm as java.lang.String, except starting with 4321
    //  4321 + s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]
    var hashCode = 4321
    var i = 0
    while (i < length) {
      hashCode = hashCode * 31 + this(i)
      i += 1
    }
    hashCode
  }

  /**
   * Checks if another object is equal to this object. They will be
   * equal if and only if the other object is a Binary containing the
   * same bytes in the same order.
   */
  override def equals(o: Any): Boolean = {
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

  /**
   * Append another Binary to this Binary, returning a new Binary as
   * the result.
   */
  def ++(other: Binary): Binary = {
    // FIXME: Needs optimization:
    // - Copy into BinaryX, if length <= 8.
    // - Try to keep the tree balanced (minimize average byte depth?).
    new CompositeBinary(this, other)
  }

  /**
   * Gets a slice of this binary, returning a new Binary as the
   * result.
   */
  override def slice(from: Int, until: Int): Binary = {
    // FIXME: Optimize.
    val sliceLength = until - from
    val array = new Array[Byte](sliceLength)
    copyToByteArray(from, array, 0, sliceLength)
    Binary(array)
  }

  /**
   * Copy this object's bytes into a given array.
   */
  def copyToByteArray(srcOffset: Int, array: Array[Byte], destOffset: Int, copyLength: Int): Unit = {
    if ((srcOffset + copyLength) > this.length || (destOffset + copyLength) > array.length)
      throw new IndexOutOfBoundsException
    var i = 0
    while (i < length) {
      array(destOffset + i) = this(srcOffset + i)
      i += 1
    }
  }

  /**
   * Get a copy of this object's bytes, stored in an array.
   */
  def toArray: Array[Byte] = {
    val array = new Array[Byte](length)
    copyToByteArray(0, array, 0, length)
    array
  }

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
