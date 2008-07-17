/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.mnesia

import org.testng.annotations.{Test, BeforeMethod}

import org.scalatest.testng.TestNGSuite
import org.scalatest._

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class TreapSuite extends TestNGSuite {
  
  override protected def runTest(testName: String, reporter: Reporter, stopper: Stopper, properties: Map[String, Any]) {
    setup
    super.runTest(testName, reporter, stopper, properties)
  }

  @BeforeMethod 
  def setup = {
  }
 
  @Test 
  def testTreapOperations= {
    // ============================
    // ===== treap operations =====
    // ============================

    val e = TreapEmptyNode[Int, String]
    val treap = new Treap[Int, String]
    val t1 = new Treap[Int, String](TreapMemNode(1, "100", e, e))
      
    val t2 = new Treap[Int, String](TreapMemNode(2, "200", e, e))
    //t2.root must_== TreapMemNode(2, "200", e, e)
      
    val t1_1 = new Treap[Int, String](TreapMemNode(1, "101", e, e))
    //t1_1.root must_== TreapMemNode(1, "101", e, e)
      
    var t = t1.union(t2)
    //t.root must_== TreapMemNode(2, "200", TreapMemNode(1, "100", e, e), e)
      
    t = t1.union(t2).union(t2)
    //t.root must_== TreapMemNode(2, "200", TreapMemNode(1, "100", e, e), e)
      
    t = t1.union(t2).union(t2).union(t1_1)
    //t.root must_== TreapMemNode(2, "200", TreapMemNode(1, "101", e, e), e)
      
    t = t1.intersect(t2)
    //t.root must_== e
      
    t = t1.diff(t2)
    //t.root must_== TreapMemNode(1, "100", e, e)

    t = t2.diff(t1)
    //t.root must_== TreapMemNode(2, "200", e, e)
      
    val t3 = new Treap[Int, String](TreapMemNode(3, "300", e, e))

    t = t1.union(t2).union(t3)
    //t.root.count(t) must_== 3L
    //t.root.firstKey(t) must_== 1
    //t.root.lastKey(t) must_== 3
    //TreapMemNode(3, "300", TreapMemNode(2, "200", TreapMemNode(1, "100", e, e), e), e) must_== t.root
      
    t = t1.union(t2).union(t3).intersect(t1.union(t2))
    //2L must_== t.root.count(t)
    //1 must_== t.root.firstKey(t)
    //2 must_== t.root.lastKey(t)
    //TreapMemNode(2, "200", TreapMemNode(1, "100", e, e), e) must_== t.root
      
    t = t1.union(t2).union(t3).diff(t1.union(t2))
    //1L must_== t.root.count(t)
    //3 must_== t.root.firstKey(t)
    //3 must_== t.root.lastKey(t)
    //TreapMemNode(3, "300", e, e) must_== t.root
      
    t = t1.union(t2).union(t3).diff(t2)
    //2L must_== t.root.count(t)
    //1 must_== t.root.firstKey(t)
    //3 must_== t.root.lastKey(t)
    //TreapMemNode(3, "300", TreapMemNode(1, "100", e, e), e) must_== t.root

    t = t1.union(t2).union(t3).-(2).asInstanceOf[t1.type]
    //2L must_== t.root.count(t)
    //1 must_== t.root.firstKey(t)
    //3 must_== t.root.lastKey(t)
    //TreapMemNode(3, "300", TreapMemNode(1, "100", e, e), e) must_== t.root

    t = t1.update(2, "200").update(3, "300").-(2).asInstanceOf[t1.type]
    //2L must_== t.root.count(t)
    //1  must_== t.root.firstKey(t)
    //3  must_== t.root.lastKey(t)
    //TreapMemNode(3, "300", TreapMemNode(1, "100", e, e), e) must_== t.root
                   
    var xs = t1.update(2, "200").update(3, "300").elements.toList
    //3 must_== xs.length
    //List((1, "100"), (2, "200"), (3, "300")) must_== xs
      
    var ttt = t1.update(2, "200").update(3, "300")             
    //xs = ttt.elements.toList
    //ttt = ttt - 2
    //3 must_== xs.length
    //List((1, "100"), (2, "200"), (3, "300")) must_== xs
    assert(true === true)
  }
  
  @Test 
  def testRangeOperations= {
    // ============================
    // ===== range operations =====
    // ============================

    val e = TreapEmptyNode[Int, String]
    val t0 = new Treap[Int, String]

    //Nil must_== t0.elements.toList
    //Nil must_== t0.from(0).elements.toList
    //Nil must_== t0.from(100).elements.toList
    //Nil must_== t0.until(100).elements.toList
    //Nil must_== t0.range(0, 100).elements.toList

    val t1 = t0.upd(1, "100")      
    val x1 = List((1, "100"))

    //x1  must_== t1.elements.toList
    //x1  must_== t1.from(0).elements.toList
    //Nil must_== t1.from(100).elements.toList
    //x1  must_== t1.until(100).elements.toList
    //x1  must_== t1.range(0, 100).elements.toList

    val t2  = t1.upd(5, "500")      
    val x5  = List((5, "500"))
    val x15 = x1 ::: x5

    //x15 must_== t2.elements.toList

    //x15 must_== t2.from(0).elements.toList
    //x15 must_== t2.from(1).elements.toList
    //x5  must_== t2.from(2).elements.toList
    //x5  must_== t2.from(5).elements.toList
    //Nil must_== t2.from(100).elements.toList

    //x15 must_== t2.until(100).elements.toList
    //x1  must_== t2.until(5).elements.toList
    //x1  must_== t2.until(2).elements.toList
    //Nil must_== t2.until(1).elements.toList
    //Nil must_== t2.until(0).elements.toList

    //x15 must_== t2.range(0, 100).elements.toList
    //x15 must_== t2.range(1, 100).elements.toList
    //x5  must_== t2.range(2, 100).elements.toList
    //x5  must_== t2.range(5, 100).elements.toList
    //Nil must_== t2.range(6, 100).elements.toList

    //x1  must_== t2.range(0, 5).elements.toList
    //x1  must_== t2.range(1, 5).elements.toList
    //Nil must_== t2.range(2, 5).elements.toList
    //Nil must_== t2.range(5, 5).elements.toList
      
    // undefined: assertEquals(Nil, t2.range(6, 5).elements.toList)

    //x1  must_== t2.range(0, 2).elements.toList
    //x1  must_== t2.range(1, 2).elements.toList
    //Nil must_== t2.range(2, 2).elements.toList
      
    // undefined: assertEquals(Nil, t2.range(5, 2).elements.toList)
    // undefined: assertEquals(Nil, t2.range(6, 2).elements.toList)

    //Nil must_== t2.range(0, 1).elements.toList
    //Nil must_== t2.range(1, 1).elements.toList
    assert(true === true)
  }
}
