/**
 * Copyright 2008 Steve Yen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package scala.actors.db

import scala.collection._

/**
 * An in-memory, immutable treap implementation in scala.
 *
 * See: http://www.cs.cmu.edu/afs/cs.cmu.edu/project/scandal/public/papers/treaps-spaa98.pdf
 */
class Treap[A <% Ordered[A], B <: AnyRef](val root: TreapNode[A, B])
  extends TreapBase[A, B]
     with immutable.SortedMap[A, B]
{
  def this() = this(TreapEmptyNode[A, B])

  def mkTreap(r: TreapNode[A, B]): Treap[A, B] = new Treap(r)

  def mkLeaf(k: A, v: B): TreapNode[A, B] =
    TreapMemNode(k, v,
                 TreapEmptyNode[A, B],
                 TreapEmptyNode[A, B])

  def mkNode(basis: TreapFullNode[A, B],
             left:  TreapNode[A, B],
             right: TreapNode[A, B]): TreapNode[A, B] = basis match {
    case TreapMemNode(k, v, _, _) =>
         TreapMemNode(k, v, left, right)
  }

  def union(that: Treap[A, B]): Treap[A, B]     = union(that.root)
  def intersect(that: Treap[A, B]): Treap[A, B] = intersect(that.root)
  def diff(that: Treap[A, B]): Treap[A, B]      = diff(that.root)

  def union(that: TreapNode[A, B]): Treap[A, B]     = mkTreap(root.union(this, that))
  def intersect(that: TreapNode[A, B]): Treap[A, B] = mkTreap(root.intersect(this, that))
  def diff(that: TreapNode[A, B]): Treap[A, B]      = mkTreap(root.diff(this, that))

  lazy val count = root.count(this) // TODO: Revisit treap size/count.

  def size: Int = count.toInt

  override def empty[C]: immutable.SortedMap[A, C] =
    throw new RuntimeException("TODO: treap empty method is unimplemented")

  override def get(key: A): Option[B] =
    root.lookup(this, key) match {
      case n: TreapFullNode[A, B] => Some(n.value(this))
      case _ => None
    }

  def elements: Iterator[Pair[A, B]] = root.elements(this).elements

  override def rangeImpl(from: Option[A], until: Option[A]): immutable.SortedMap[A, B] =
    mkTreap(root.range(this, from, until))

  override def update[B1 >: B](key: A, value: B1): immutable.SortedMap[A, B1] =
    value match {
      case v: B => upd(key, v)
      case _ => throw new RuntimeException("TODO: wrong treap update type")
    }

  def upd(key: A, value: B): Treap[A, B] =
    union(mkLeaf(key, value))

  override def - (key: A): immutable.SortedMap[A, B] =
    del(key)

  def del(key: A): Treap[A, B] =
    mkTreap(root.del(this, key))

  override def firstKey: A = root.firstKey(this)
  override def lastKey: A  = root.lastKey(this)

  override def compare(k0: A, k1: A): Int = k0.compare(k1)

  override def toString = root.toString

  /**
   * Subclasses will definitely want to consider overriding this
   * weak priority calculation.  Consider, for example, leveraging
   * the treap's heap-like ability to shuffle high-priority
   * nodes to the top of the heap for faster access.
   */
  def priority(node: TreapFullNode[A, B]): Int = {
    val h = node.key.hashCode
    ((h << 16) & 0xffff0000) | ((h >> 16) & 0x0000ffff)
  }
}

// ---------------------------------------------------------

abstract class TreapBase[A <% Ordered[A], B <: AnyRef]  {
  def mkLeaf(k: A, v: B): TreapNode[A, B]

  def mkNode(basis: TreapFullNode[A, B],
             left:  TreapNode[A, B],
             right: TreapNode[A, B]): TreapNode[A, B]

  def compare(x: A, y: A): Int
  def priority(n: TreapFullNode[A, B]): Int
}

// ---------------------------------------------------------

abstract class TreapNode[A <% Ordered[A], B <: AnyRef]
{
  type T     = TreapBase[A, B]
  type Node  = TreapNode[A, B]
  type Full  = TreapFullNode[A, B]
  type Empty = TreapEmptyNode[A, B]

  def isEmpty: Boolean
  def isLeaf(t: T): Boolean

  def count(t: T): Long
  def firstKey(t: T): A
  def lastKey(t: T): A

  def lookup(t: T, s: A): Node

  /**
   * Splits a treap into two treaps based on a split key "s".
   * The result tuple-3 means (left, X, right), where X is either...
   * null - meaning the key "s" was not in the original treap.
   * non-null - returning the Full node that had key "s".
   * The tuple-3's left treap has keys all < s,
   * and the tuple-3's right treap has keys all > s.
   */
  def split(t: T, s: A): (Node, Full, Node)

  /**
   * For join to work, we require that "this".keys < "that".keys.
   */
  def join(t: T, that: Node): Node

  /**
   * When union'ed, the values from "that" have precedence
   * over "this" when there are matching keys.
   */
  def union(t: T, that: Node): Node

  /**
   * When intersect'ed, the values from "that" have precedence
   * over "this" when there are matching keys.
   */
  def intersect(t: T, that: Node): Node

  /**
   * Works like set-difference, as in "this" minus "that", or this - that.
   */
  def diff(t: T, that: Node): Node

  def elements(t: T): immutable.ImmutableIterator[Pair[A, B]]

  def range(t: T, from: Option[A], until: Option[A]): TreapNode[A, B]

  def del(t: T, k: A): TreapNode[A, B]
}

// ---------------------------------------------------------

case class TreapEmptyNode[A <% Ordered[A], B <: AnyRef] extends TreapNode[A, B]
{
  def isEmpty: Boolean = true
  def isLeaf(t: T): Boolean  = throw new RuntimeException("isLeaf on empty treap node")

  def count(t: T)    = 0L
  def firstKey(t: T) = throw new NoSuchElementException("empty treap")
  def lastKey(t: T)  = throw new NoSuchElementException("empty treap")

  def lookup(t: T, s: A): Node          = this
  def split(t: T, s: A)                 = (this, null, this)
  def join(t: T, that: Node): Node      = that
  def union(t: T, that: Node): Node     = that
  def intersect(t: T, that: Node): Node = this
  def diff(t: T, that: Node): Node      = this

  def elements(t: T): immutable.ImmutableIterator[Pair[A, Nothing]] =
                      immutable.ImmutableIterator.empty

  def range(t: T, from: Option[A], until: Option[A]): TreapNode[A, B] = this

  def del(t: T, k: A): TreapNode[A, B] = this

  override def toString = "_"
}

// ---------------------------------------------------------

abstract class TreapFullNode[A <% Ordered[A], B <: AnyRef] extends TreapNode[A, B]
{
  def key: A
  def left(t: T): Node
  def right(t: T): Node
  def value(t: T): B

  def priority(t: T) = t.priority(this)

  def isEmpty: Boolean = false
  def isLeaf(t: T): Boolean = left(t).isEmpty && right(t).isEmpty

  def count(t: T)    = 1L + left(t).count(t) + right(t).count(t)
  def firstKey(t: T) = if (left(t).isEmpty)  key else left(t).firstKey(t)
  def lastKey(t: T)  = if (right(t).isEmpty) key else right(t).lastKey(t)

  def lookup(t: T, s: A): Node = {
    val c = t.compare(s, key)
    if (c == 0)
      this
    else {
      if (c < 0)
        left(t).lookup(t, s)
      else
        right(t).lookup(t, s)
    }
  }

  def split(t: T, s: A) = {
    val c = t.compare(s, key)
    if (c == 0) {
      (left(t), this, right(t))
    } else {
      if (c < 0) {
        if (isLeaf(t))
          (left(t), null, this) // Optimization when isLeaf.
        else {
          val (l1, m, r1) = left(t).split(t, s)
          (l1, m, t.mkNode(this, r1, right(t)))
        }
      } else {
        if (isLeaf(t))
          (this, null, right(t)) // Optimization when isLeaf.
        else {
          val (l1, m, r1) = right(t).split(t, s)
          (t.mkNode(this, left(t), l1), m, r1)
        }
      }
    }
  }

  def join(t: T, that: Node): Node = that match {
    case e: Empty => this
    case b: Full =>
      if (priority(t) > b.priority(t))
        t.mkNode(this, left(t), right(t).join(t, b))
      else
        t.mkNode(b, this.join(t, b.left(t)), b.right(t))
  }

  def union(t: T, that: Node): Node = that match {
    case e: Empty => this
    case b: Full =>
      if (priority(t) > b.priority(t)) {
        val (l, m, r) = b.split(t, key)
        if (m == null)
          t.mkNode(this, left(t).union(t, l), right(t).union(t, r))
        else
          t.mkNode(m, left(t).union(t, l), right(t).union(t, r))
      } else {
        val (l, m, r) = this.split(t, b.key)

        // Note we don't use m because b (that) has precendence over this when union'ed.
        //
        t.mkNode(b, l.union(t, b.left(t)), r.union(t, b.right(t)))
      }
  }

  def intersect(t: T, that: Node): Node = that match {
    case e: Empty => e
    case b: Full =>
      if (priority(t) > b.priority(t)) {
        val (l, m, r) = b.split(t, key)
        val nl = left(t).intersect(t, l)
        val nr = right(t).intersect(t, r)
        if (m == null)
          nl.join(t, nr)
        else
          t.mkNode(m, nl, nr)
      } else {
        val (l, m, r) = this.split(t, b.key)
        val nl = l.intersect(t, b.left(t))
        val nr = r.intersect(t, b.right(t))
        if (m == null)
          nl.join(t, nr)
        else
          t.mkNode(b, nl, nr) // The b value has precendence over this value.
      }
  }

  def diff(t: T, that: Node): Node = that match {
    case e: Empty => this
    case b: Full =>
      // TODO: Need to research alternative "diffb" algorithm from scandal...
      //       http://www.cs.cmu.edu/~scandal/treaps/src/treap.sml
      //
      val (l2, m, r2) = b.split(t, key)
      val l = left(t).diff(t, l2)
      val r = right(t).diff(t, r2)
      if (m == null)
        t.mkNode(this, l, r)
      else
        l.join(t, r)
  }

  def elements(t: T): immutable.ImmutableIterator[Pair[A, B]] =
    left(t).elements(t).append(Pair(key, value(t)), () => right(t).elements(t))

  def range(t: T, from: Option[A], until: Option[A]): TreapNode[A, B] = {
    if (from == None && until == None)
      return this

    if (from  != None && t.compare(key, from.get) < 0)   return right(t).range(t, from, until)
    if (until != None && t.compare(key, until.get) >= 0) return left(t).range(t, from, until)

    val (l1, m1, r1) = from.map(s => left(t).split(t, s)).
                            getOrElse(null, null, left(t))
    val (l2, m2, r2) = until.map(s => right(t).split(t, s)).
                             getOrElse(right(t), null, null)
    if (m1 == null)
      t.mkNode(this, r1, l2)
    else
      t.mkNode(m1, TreapEmptyNode[A, B],
                   TreapEmptyNode[A, B]).
        join(t, t.mkNode(this, r1, l2))
  }

  def del(t: T, k: A): TreapNode[A, B] = {
    val (l, m, r) = split(t, k)
    l.join(t, r)
  }
}

// ---------------------------------------------------------

case class TreapMemNode[A <% Ordered[A], B <: AnyRef](key: A, v: B, nl: TreapNode[A, B], nr: TreapNode[A, B])
   extends TreapFullNode[A, B] {
  def left(t: T)  = nl
  def right(t: T) = nr
  def value(t: T) = v
}


