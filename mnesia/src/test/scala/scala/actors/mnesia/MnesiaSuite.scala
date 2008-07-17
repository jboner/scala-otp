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
class MnesiaSuite extends TestNGSuite {
  case class Person(name: String)
  case class Address(street: String, number: String, zipcode: Int, city: String, country: String)

  Mnesia.
    createTable("person", classOf[Person]).
    createTable("address", classOf[Address])

  override protected def runTest(testName: String, reporter: Reporter, stopper: Stopper, properties: Map[String, Any]) {
    setup
    super.runTest(testName, reporter, stopper, properties)
  }

  @BeforeMethod 
  def setup = Mnesia.clear
 
  @Test 
  def testCreateMultipleTablesWithSameName= {      
    intercept(classOf[IllegalArgumentException]) {
      Mnesia.createTable("person", classOf[Person])
    }
    assert(true === true)
  }

  @Test 
  def testPersist= {      
    Mnesia.persist("person", Person("Jonas"))
    Mnesia.persist("person", Person("Sara"))
    Mnesia.persist("person", Person("Kalle"))
      
    val persons = Mnesia findAll "person"
    assert(persons.size === 3)
    assert(true === true)
  }
}
