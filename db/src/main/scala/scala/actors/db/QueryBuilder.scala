/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.db

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class QueryBuilder[K <% Ordered[K], V <: AnyRef](val treap: Treap[K, V], val db: DB) {

  def range(from: Option[K], until: Option[K]): Treap[K, V] = treap.rangeImpl(from, until).asInstanceOf[Treap[K, V]]
    
  def union(treap1: Treap[K, V], treap2: Treap[K, V]): Treap[K, V] = treap1.union(treap2)

  def intersect(treap1: Treap[K, V], treap2: Treap[K, V]): Treap[K, V] = treap1.intersect(treap2)

  def diff(treap1: Treap[K, V], treap2: Treap[K, V]): Treap[K, V] = treap1.diff(treap2)  
}

// val q1 = new QueryBuilder[Int, String](classOf[Person])
// val q2 = new QueryBuilder[Int, String](classOf[Person])

// q1.union(q1.rangePK(None, 100), q2.rangeIndex(1, 200, "id")) 


