  /**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.mnesia

import scala.actors.mnesia.Index._

import org.testng.annotations.{Test, BeforeMethod}

import org.scalatest.testng.TestNGSuite
import org.scalatest._

case class Person(name: String)
case class Address(street: String, number: String, zipcode: Int, city: String, country: String)

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class DBSuite extends TestNGSuite {
  val person = classOf[Person]
  val address = classOf[Address]

  DB.
  init(Config(InMemoryStorageStrategy)).
  start.
  createTable(classOf[Person]).
  createTable(classOf[Address])

  override protected def runTest(testName: String, reporter: Reporter, stopper: Stopper, properties: Map[String, Any]) {
    setup
    super.runTest(testName, reporter, stopper, properties)
  }

  @BeforeMethod
  def setup = DB.clear

  //@Test
  def testCreateMultipleTablesWithSameName = {
    intercept(classOf[IllegalArgumentException]) {
      DB.createTable(classOf[Person])
    }
    assert(true === true)
  }

  @Test
  def testAddIndexToEmptyTable = {
    DB.addIndex("name", person, (v: Any) => StringIndex(v.asInstanceOf[String]))
    assert(true === true)
  }

  @Test
  def testAddIndexNonEmptyTable = {
    DB.store(Person("Jonas"))
    DB.store(Person("Sara"))
    DB.store(Person("Kalle"))
    DB.addIndex("name", person, (v: Any) => StringIndex(v.asInstanceOf[String]))
    assert(true === true)
  }

  @Test
  def testStoreFindAll = {
    DB.store(Person("Jonas"))
    DB.store(Person("Sara"))
    DB.store(Person("Kalle"))

    val persons: List[Person] = DB findAll person
    assert(persons.size === 3)
    assert(persons(0).name === "Jonas")
    assert(persons(1).name === "Sara")
    assert(persons(2).name === "Kalle")
    assert(true === true)
  }

  @Test
  def testRemoveByPK = {
    val jonas = DB.store(Person("Jonas"))
    val sara = DB.store(Person("Sara"))
    val kalle = DB.store(Person("Kalle"))

    // remove by PK
      DB.remove(jonas)
    val persons1: List[Person] = DB findAll person
    assert(persons1.size === 2)

    DB.remove(kalle)
    val persons2: List[Person] = DB findAll person
    assert(persons2.size === 1)
    assert(persons2(0).name === "Sara")

    assert(true === true)
  }

  @Test
  def testRemoveByRef = {
    val jonas = DB.store(Person("Jonas"))
    val sara = DB.store(Person("Sara"))
    val kalle = DB.store(Person("Kalle"))

    // remove by instance
    DB.remove(Person("Kalle"))
    val persons: List[Person] = DB findAll person
    assert(persons.size === 2)
    assert(persons(0).name === "Jonas")
    assert(persons(1).name === "Sara")

    assert(true === true)
  }

  @Test
  def testFindByPK = {
    val pk = DB.store(Person("Jonas"))
    val jonas: Person = DB.findByPK(pk).getOrElse(fail("failed findByPK"))
    assert(jonas.name == "Jonas")
  }

  @Test
  def testFindByIndex = {
    DB.store(Person("Jonas"))
    DB.addIndex("name", person, (v: Any) => StringIndex(v.asInstanceOf[String]))
    val entities: List[Person] = DB.findByIndex("Jonas", Column ("name", person))
    assert(entities.size == 1)
    assert(entities(0).name == "Jonas")
  }
}
