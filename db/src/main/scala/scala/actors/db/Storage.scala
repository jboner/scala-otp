/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.db

import scala.actors.Actor
import scala.actors.Actor._
import scala.actors.behavior._
import scala.actors.behavior.Helpers._

import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}
import java.lang.reflect.Field
import java.lang.System.{currentTimeMillis => timeNow}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
private[db] class InMemoryStorage(schema: CovariantMap[Class[_], Table]) extends Storage(schema)

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
private[db] abstract class Storage(protected val schema: CovariantMap[Class[_], Table]) {

  protected val db = Map[Column, Treap[Index, AnyRef]]()
  protected val indices = Map[Column, Treap[Index, Map[Index, AnyRef]]]()

  /**
   * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
   */
  private[db] object PKFactory {
    val keys = new CovariantMap[Class[_], AtomicLong]
    val indexFactory = (key: Any) => key.asInstanceOf[PK]

    def init(table: Class[_]) = {
      val currrentIndex =
        if (db.contains(getPKColumnFor(table))) {
          val treap = db(getPKColumnFor(table))
          if (!treap.isEmpty) treap.lastKey.value.asInstanceOf[Long] else 1L
        } else 1L
      keys(table) = new AtomicLong(currrentIndex)
    }

    def next(table: Class[_]): PK = {
      if (!keys.contains(table)) init(table) 
      val pk = PK(keys(table).getAndIncrement)
      pk.table = table
      pk
    }
  }

  def createTable(table: Class[_], pkName: String) = {
    try {
      val pkColumn = Column(pkName, table)
      val columns: List[Column] = for (field <- table.getDeclaredFields.toList if field.getName != "$outer")
                                  yield Column(field.getName, field.getType)
      log.info("Creating table <{}> with columns <{}>", table.getName, columns)
      
      val pkField = table.getDeclaredFields.toList.find(field => field.getName == pkName).getOrElse(throw new IllegalArgumentException("Primary key field <" + pkName + "> does not exist"))
      if (pkField.getType != classOf[PK]) throw new IllegalArgumentException("Primary key field <" + pkName + "> for table <" + table.getName + "> has to be of type <PK>")
      
      if (schema.contains(table)) throw new IllegalArgumentException("Table <" + table.getName + "> already exists")
      schema(table) = Table(table, columns, pkColumn, PKFactory.indexFactory, GeneratePrimaryKeySequence(), Nil)

      db += getPKColumnFor(table) -> new Treap[Index, AnyRef] 
      addIndex(pkName, table, PKFactory.indexFactory)    
    } catch { case e => e.printStackTrace; e }
  }

  def createTable(table: Class[_], pkName: String, pkIndexFactory: (Any) => Index) = {
    val pkColumn = Column(pkName, table)
    val columns: List[Column] = for (field <- table.getDeclaredFields.toList if field.getName != "$outer")
                                yield Column(field.getName, field.getType)
    log.info("Creating table <{}> with columns <{}>", table.getName, columns)

    if (schema.contains(table)) throw new IllegalArgumentException("Table <" + table.getName + "> already exists")
    schema(table) = Table(table, columns, pkColumn, pkIndexFactory, CustomPrimaryKeySequence(), Nil)

    db += getPKColumnFor(table) -> new Treap[Index, AnyRef] 
    addIndex(pkName, table, pkIndexFactory)    
  }

  def addIndex(columnName: String, table: Class[_], indexFactory: (Any) => Index) = {
    val field = try { 
      val field = table.getDeclaredField(columnName) 
      field.setAccessible(true)
      field
    } catch { 
      case e => throw new IllegalArgumentException("Could not get index <" + columnName + "> for table <" + table.getName + "> due to: " + e.toString) 
    }

    val currentDB = db(getPKColumnFor(table))
    val emptyTreap = new Treap[Index, Map[Index, AnyRef]]()

    val fullTreap = currentDB.elements.foldLeft(emptyTreap) { (indexTreap, entry) => 
      val entity = entry._2
      val index = indexFactory(field.get(entity))
      indexTreap.get(index) match {
        case Some(entities) =>
          entities += getPKIndexFor(entity) -> entity
          indexTreap
        case None =>
          indexTreap.upd(index, Map[Index, AnyRef](getPKIndexFor(entity) -> entity))
      }
    }

    val column = Column(columnName, table)

    // add index to index table
    indices += column -> fullTreap

    // add index to schema
    val schemaTable = schema(table)
    schemaTable.indices = (column, field, indexFactory) :: schemaTable.indices

    log.info("Index <{}> for table <{}> has been compiled", columnName, table.getName)
  }

  def findTreapFor(columnName: String, table: Class[_]): AnyRef = {
    ensureTableExists(table)
    if (getPKColumnFor(table) == columnName) {
      db(getPKColumnFor(table))      
    } else {
      val column = getColumnFor(columnName, table)
      ensureIndexExists(column)
      indices(column)
    }
  }

  def store(entity: AnyRef): Index = {
    val tableClass = entity.getClass
    ensureTableExists(tableClass)
    val table = schema(tableClass)
    val pkColumn = getPKColumnFor(tableClass)

    // get and set primary key
    val pkIndex = table.pkSequenceScheme match {
      case GeneratePrimaryKeySequence() => 
        val pk = PKFactory.next(tableClass)
        setIndexFor(entity, pkColumn.name, pk)
        pk
      case CustomPrimaryKeySequence() => 
        getPKIndexFor(entity)
    }    

    // update db
    db(pkColumn) = db(pkColumn).upd(pkIndex, entity)

    // update indices
    table.indices.foreach { item =>
      val (column, field, indexFactory) = item
      val indexTreap = indices(column)
      val index = getIndexFor(entity, column.name)
      indexTreap.get(index) match {
        case Some(entities) =>
         entities += getPKIndexFor(entity) -> entity
       case None =>
          addIndex(column.name, column.table, indexFactory)
          indices(column) = indexTreap.upd(index, Map[Index, AnyRef](getPKIndexFor(entity) -> entity))
      }
    }
    pkIndex
  }

  def remove(entity: AnyRef) = {
    val table = entity.getClass
    ensureTableExists(table)
    val pkIndex = getPKIndexFor(entity)
    findByPK(pkIndex, entity.getClass).getOrElse(throw new IllegalArgumentException("Entity <" + entity + "> is not persisted and can therefore not be removed"))    
    val pkColumn = getPKColumnFor(table)

    // db
    val oldTreap = db(pkColumn)
    val newTreap = oldTreap.del(pkIndex)
    db(pkColumn) = newTreap

    // indexes
    schema(table).indices.foreach { item =>
      val (column, field, indexFactory) = item
      val indexTreap = indices(column)
      val index = getIndexFor(entity, column.name)
      indices(column) = indexTreap.del(index) 
    }
  }

  def removeByPK(pkIndex: Index, table: Class[_]) = {
    ensureTableExists(table)
    val pkColumn = getPKColumnFor(table)
    if (!db.contains(pkColumn)) throw new IllegalArgumentException("Primary key <" + pkColumn.name + "> for table <" + table.getName + "> does not exist")

    // indexes
    schema(table).indices.foreach { item =>
      val (column, field, indexFactory) = item
      if (column.table == table) { 
        val indexTreap = indices(column)
        val entity = findByPK(pkIndex, table).getOrElse(throw new IllegalStateException("No such primary key <" + pkIndex + "> for table <" + table.getName + ">"))
        val index = getIndexFor(entity, column.name)
        indices(column) = indexTreap.del(index)         
      }
    }

    // db
    val newTreap = db(pkColumn).del(pkIndex)
    db(pkColumn) = newTreap
  }

  def removeByIndex(index: Index, columnName: String, table: Class[_]) = {
    ensureTableExists(table)
    val column = getColumnFor(columnName, table)
    ensureIndexExists(column)

    // indexes
    val indexTreap = indices(column)
    val entities = indexTreap.get(index).
      getOrElse(throw new IllegalStateException("No such index <" + columnName + "> for table <" + table.getName + ">")).
      values.toList
    indices(column) = indexTreap.del(index)
  
    // db
    val pkColumn = getPKColumnFor(table)
    entities.foreach { entity => 
      val pkIndex = getPKIndexFor(entity)
      val newTreap = db(pkColumn).del(pkIndex)
      db(pkColumn) = newTreap
    }
  }

  def findByPK(pkIndex: Index, table: Class[_]): Option[AnyRef] = {
    ensureTableExists(table)
    val pkColumn = getPKColumnFor(table)
    db(pkColumn).get(pkIndex) 
  }

  def findByIndex(index: Index, columnName: String, table: Class[_]): List[AnyRef] = {
    ensureTableExists(table)
    val column = getColumnFor(columnName, table)
    ensureIndexExists(column)
    indices(column).get(index).
      getOrElse(throw new IllegalStateException("No such index <" + columnName + "> for table <" + table.getName + ">")).
      values.toList
  }

  def findAll(table: Class[_]): List[AnyRef] = {
    ensureTableExists(table)
    db(getPKColumnFor(table)).values.toList
  }

  def size(table: Class[_]): Int = db(getPKColumnFor(table)).size

  /**
   * Returns the first key of the table.
   */
  def firstPK(table: Class[_]): Index = {
    ensureTableExists(table)
    db(getPKColumnFor(table)).firstKey
  }

  /**
   * Returns the last key of the table.
   */
  def lastPK(table: Class[_]): Index = {
    ensureTableExists(table)
    db(getPKColumnFor(table)).lastKey
  }

  /**
   * Creates a ranged projection of this collection with both a lower-bound and an upper-bound.
   */
  def rangePK(from: Index, until: Index, table: Class[_]): List[AnyRef] = {
    ensureTableExists(table)
    db(getPKColumnFor(table)).range(from, until).values.toList
  }

  /**
   * Creates a ranged projection of this collection with no upper-bound.
   */
  def rangeFromPK(from: Index, table: Class[_]) : List[AnyRef] = {
    ensureTableExists(table)
    db(getPKColumnFor(table)).from(from).values.toList
  }

  /**
   * Creates a ranged projection of this collection with no lower-bound.
   */
  def rangeUntilPK(until: Index, table: Class[_]): List[AnyRef] = {
    ensureTableExists(table)
    db(getPKColumnFor(table)).until(until).values.toList
  }

  /**
   * Returns the first index of the table.
   */
  def firstIndex(columnName: String, table: Class[_]): Index = {
    ensureTableExists(table)
    val column = getColumnFor(columnName, table)
    ensureIndexExists(column)
    indices(column).firstKey
  }

  /**
   * Returns the last key of the table.
   */
  def lastIndex(columnName: String, table: Class[_]): Index = {
    ensureTableExists(table)
    val column = getColumnFor(columnName, table)
    ensureIndexExists(column)
    indices(column).lastKey
  }

  /**
   * Creates a ranged projection of this collection with both a lower-bound and an upper-bound.
   */
  def rangeIndex(from: Index, until: Index, columnName: String, table: Class[_]): List[AnyRef] = {
    ensureTableExists(table)
    val column = getColumnFor(columnName, table)
    ensureIndexExists(column)
    indices(column).range(from, until).values.toList
  }

  /**
   * Creates a ranged projection of this collection with no upper-bound.
   */
  def rangeFromIndex(from: Index, columnName: String, table: Class[_]) : List[AnyRef] = {
    ensureTableExists(table)
    val column = getColumnFor(columnName, table)
    ensureIndexExists(column)
    indices(column).from(from).values.toList
  }

  /**
   * Creates a ranged projection of this collection with no lower-bound.
   */
  def rangeUntilIndex(until: Index, columnName: String, table: Class[_]): List[AnyRef] = {
    ensureTableExists(table)
    val column = getColumnFor(columnName, table)
    ensureIndexExists(column)
    indices(column).until(until).values.toList
  }

  def clear = { 
    db.clear
    indices.clear
    for (table <- schema.values) db += getPKColumnFor(table.clazz) -> new Treap[Index, AnyRef]
    for {
      table <- schema.values
      (column, field, indexFactory) <- table.indices
    } indices += column -> new Treap[Index, Map[Index, AnyRef]]()
  }

  protected def getPKColumnFor(table: Class[_]): Column = {
    if (!schema.contains(table)) throw new IllegalStateException("Table <" + table.getName + "> does not exist")
    schema(table).pk
  }

  protected def getColumnFor(name: String, table: Class[_]): Column = {
    if (!schema.contains(table)) throw new IllegalStateException("Table <" + table.getName + "> does not exist")
    Column(name, table)
  }

  protected def getPKIndexFor(entity: AnyRef): Index = {
    val tableClass = entity.getClass
    if (!schema.contains(tableClass)) throw new IllegalStateException("Table <" + tableClass.getName + "> does not exist")
    getIndexFor(entity, schema(tableClass).pk.name)
  }

  protected def getIndexFor(entity: AnyRef, columnName: String): Index = {
    try { 
      val (_, field, indexFactory) = schema(entity.getClass).indices.find(item => item._1.name == columnName).getOrElse(throw new IllegalArgumentException("Could not get primary key <" + columnName + "> for table <" + entity.getClass.getName + ">: no such column"))
      indexFactory(field.get(entity))
    } catch { 
      case e => throw new IllegalArgumentException("Could not get index <" + columnName + "> for table <" + entity.getClass.getName + "> due to: " + e.toString) 
    }
  }

  protected def setIndexFor(entity: AnyRef, columnName: String, index: Index) = {
    try { 
      val (_, field, _) = schema(entity.getClass).indices.find(item => item._1.name == columnName).getOrElse(throw new IllegalArgumentException("Could not get primary key <" + columnName + "> for table <" + entity.getClass.getName + ">: no such column"))
      field.set(entity, index)
    } catch { 
      case e => throw new IllegalArgumentException("Could not get primary key <" + columnName + "> for table <" + entity.getClass.getName + "> due to: " + e.toString) 
    }
  }

  protected def ensureTableExists(table: Class[_]) = {
    if (!db.contains(Column(schema(table).pk.name, table))) throw new IllegalArgumentException("Table [" + table.getName + "] does not exist")
  }

  protected def ensureIndexExists(index: Column) = {
    if (!indices.contains(index)) throw new IllegalArgumentException("Index [" + index + "] does not exist")
  }
}

