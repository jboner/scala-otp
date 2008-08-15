  /**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.mnesia

import scala.actors.mnesia.Index._

import org.testng.annotations.{BeforeSuite, BeforeMethod, Test}
import org.testng.Assert._

import org.scalatest.testng.TestNGSuite
import org.scalatest._

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class InMemoryGeneratedPKSuite extends TestNGSuite {
  case class Person(name: String) {
    var id: PK = _
    override def toString = "Person: " + name + " " + id + " " + System.identityHashCode(this)
  }
  val person = classOf[Person]

  DB.
  init(Config(InMemoryStorageStrategy)).
  start.
  createTable(classOf[Person], "id")

  override protected def runTest(testName: String, reporter: Reporter, stopper: Stopper, properties: Map[String, Any]) {
    setup
    super.runTest(testName, reporter, stopper, properties)
  }

  @BeforeMethod { val groups=Array("unit") }
  def setup = DB.clear

  //@Test { val groups=Array("unit") }
  def testCreateMultipleTablesWithSameName = {
    intercept(classOf[IllegalArgumentException]) {
      DB.createTable(classOf[Person], "name")
    }
    assert(true === true)
  }

  @Test { val groups=Array("unit") }
  def testAddIndexToEmptyTable = {
    DB.addIndex("name", person, (v: Any) => StringIndex(v.asInstanceOf[String]))
    assert(true === true)
  }

  @Test { val groups=Array("unit") }
  def testAddIndexNonEmptyTable = {
    DB.store(Person("Jonas"))
    DB.store(Person("Sara"))
    DB.store(Person("Kalle"))
    DB.addIndex("name", person, (v: Any) => StringIndex(v.asInstanceOf[String]))
    assert(true === true)
  }

  @Test { val groups=Array("unit") }
  def testStoreFindAll = {
    DB.store(Person("Jonas"))
    DB.store(Person("Sara"))
    DB.store(Person("Kalle"))

    val persons: List[Person] = DB findAll person
    assert(persons.size === 3)
    assert(persons.exists(_.name == "Jonas"))
    assert(persons.exists(_.name == "Sara"))
    assert(persons.exists(_.name == "Kalle"))
    assert(true === true)
  }

  @Test { val groups=Array("unit") }
  def testRemoveByPK = {
    val pkJonas = DB.store(Person("Jonas"))
    val pkSara = DB.store(Person("Sara"))
    val pkKalle = DB.store(Person("Kalle"))

    Thread.sleep(1000)
    // remove by PK
    DB.remove(pkJonas, person)
    val persons1: List[Person] = DB findAll person
    assert(persons1.size === 2)

    DB.remove(pkKalle, person)
    val persons2: List[Person] = DB findAll person
    assert(persons2.size === 1)
    assert(persons2(0).name === "Sara")

    assert(true === true)
  }

  @Test { val groups=Array("unit") }
  def testRemoveByRef = {
    val kalle = Person("Kalle")
    DB.store(kalle)
    DB.store(Person("Jonas"))
    DB.store(Person("Sara"))

    // remove by instance
    DB.remove(kalle)
    val persons: List[Person] = DB findAll person
    assert(persons.size === 2)
    assert(persons.exists(_.name == "Jonas"))
    assert(persons.exists(_.name == "Sara"))

    assert(!persons.exists(_.name == "Kalle"))

    assert(true === true)
  }

  @Test { val groups=Array("unit") }
  def testFindByPK = {
    val pk = DB.store(Person("Jonas"))
    val jonas: Person = DB.findByPK(pk, person).getOrElse(fail("failed findByPK"))
    assert(jonas.name == "Jonas")
  }

  @Test { val groups=Array("unit") }
  def testFindByIndex = {
    DB.store(Person("Jonas"))
    DB.addIndex("name", person, (v: Any) => StringIndex(v.asInstanceOf[String]))
    val entities: List[Person] = DB.findByIndex("Jonas", "name", person)
    assert(entities.size == 1)
    assert(entities(0).name == "Jonas")
  }
}
