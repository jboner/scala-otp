  /**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.mnesia

import org.testng.annotations.{Test, BeforeMethod}

import org.scalatest.testng.TestNGSuite
import org.scalatest._

case class Person(name: String)
case class Address(street: String, number: String, zipcode: Int, city: String, country: String)

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class MnesiaSuite extends TestNGSuite {
  val person = classOf[Person]
  val address = classOf[Address]

  Mnesia.start
  Mnesia.createTable(classOf[Person]).createTable(classOf[Address])
  
  override protected def runTest(testName: String, reporter: Reporter, stopper: Stopper, properties: Map[String, Any]) {
    setup
    super.runTest(testName, reporter, stopper, properties)
  }

  @BeforeMethod
  def setup = { 
    Mnesia.clear
  }
  
  //@Test
  def testCreateMultipleTablesWithSameName = {
    intercept(classOf[IllegalArgumentException]) {
      Mnesia.createTable(classOf[Person])
    }
    assert(true === true)
  }

  @Test
  def testAddIndexToEmptyTable = {
    Mnesia.addIndex[String]("name", person, StringIndex.newInstance)
  }

  @Test
  def testAddIndexNonEmptyTable = {
    Mnesia.store(Person("Jonas"))
    Mnesia.store(Person("Sara"))
    Mnesia.store(Person("Kalle"))
    Mnesia.addIndex[String]("name", person, StringIndex.newInstance)
  }

  @Test
  def testStoreFindAll = {
    Mnesia.store(Person("Jonas"))
    Mnesia.store(Person("Sara"))
    Mnesia.store(Person("Kalle"))

    val persons = Mnesia findAll person
    assert(persons.size === 3)
    assert(true === true)
  }

  @Test
  def testRemoveByPK = {
    val jonas = Mnesia.store(Person("Jonas"))
    val sara = Mnesia.store(Person("Sara"))
    val kalle = Mnesia.store(Person("Kalle"))

    // remove by PK
    Mnesia.remove(jonas)
    val persons1 = Mnesia findAll person
    assert(persons1.size === 2)

    Mnesia.remove(kalle)
    val persons2 = Mnesia findAll person
    assert(persons2.size === 1)
    assert(true === true)
  }

  @Test
  def testRemoveByRef = {
    val jonas = Mnesia.store(Person("Jonas"))
    val sara = Mnesia.store(Person("Sara"))
    val kalle = Mnesia.store(Person("Kalle"))

    // remove by instance
    Mnesia.remove(Person("Kalle"))
    val persons = Mnesia findAll person
    assert(persons.size === 2)

    assert(true === true)
  }

  @Test
  def testFindByPK = {
    val pk = Mnesia.store(Person("Jonas"))
    val jonas = Mnesia.findByPK(pk).getOrElse(fail("failed findByPK")).asInstanceOf[Person]
    assert(jonas.name == "Jonas")
  }

  @Test
  def testFindByIndex = {
    Mnesia.store(Person("Jonas"))
    Mnesia.addIndex[String]("name", person, StringIndex.newInstance)
    val jonas = Mnesia.findByIndex(StringIndex("Jonas"), Column ("name", person)).getOrElse(fail("failed findByIndex")).asInstanceOf[Person]
    assert(jonas.name == "Jonas")
  }
}
