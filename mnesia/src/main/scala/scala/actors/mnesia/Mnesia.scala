/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.mnesia

import scala.actors.Actor._
import scala.actors.behavior._
import scala.actors.behavior.Helpers._
import scala.collection.mutable.Map

import java.util.concurrent.atomic.AtomicLong

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
sealed abstract case class MnesiaMessage

// ------------------------
// --- REQUEST MESSAGES ---
// ------------------------
case class CreateTable(table: Class[_]) extends MnesiaMessage
case class AddIndex[I](name: String, table: Class[_], indexFactory: (I) => Index) extends MnesiaMessage
case class ChangeSchema(schema: String) extends MnesiaMessage
case object Clear extends MnesiaMessage

case class RemoveByEntity(entity: AnyRef) extends MnesiaMessage
case class RemoveByPK(pk: PK) extends MnesiaMessage

/** Replies with PrimaryKey(..) */
case class Store(entity: AnyRef) extends MnesiaMessage
/** Replies with Entity(..) */
case class FindByPK(pk: PK) extends MnesiaMessage
/** Replies with Entity(..) */
case class FindByIndex(index: Index, column: Column) extends MnesiaMessage
/** Replies with Entities(..) */
case class FindAll(table: Class[_]) extends MnesiaMessage

//-----------------------
// --- REPLY MESSAGES ---
//-----------------------
case class PrimaryKey(pk: PK) extends MnesiaMessage
case class Entity(entity: Option[AnyRef]) extends MnesiaMessage
case class Entities(list: List[AnyRef]) extends MnesiaMessage
case object Success extends MnesiaMessage
case class Failure(message: String, cause: Option[Throwable]) extends MnesiaMessage

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class MnesiaException(val message: String) extends RuntimeException {
  override def toString: String = "scala.actors.mnesia.MnesiaException: " + message
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object Mnesia extends Logging {

  def start = supervisor ! Start
  
  def stop = supervisor ! Stop

  def createTable(table: Class[_]): Mnesia.type = {
    mnesia ! CreateTable(table)
    this
  }

  def addIndex[I](name: String, table: Class[_], indexFactory: (I) => Index) = mnesia ! AddIndex(name, table, indexFactory)

  def store(entity: AnyRef): PK = {
    val result: Option[PrimaryKey] = mnesia !!! Store(entity) 
    result match {
      case Some(PrimaryKey(pk)) => pk
      case None => throw new MnesiaException("Could not store entity <" + entity + ">")  
    }
  }
  
  def remove(entity: AnyRef) = mnesia ! RemoveByEntity(entity)

  def remove(pk: PK) = mnesia ! RemoveByPK(pk)

  def findByPK(pk: PK): Option[AnyRef] = {
    val result: Option[Entity] = mnesia !!! FindByPK(pk) 
    result match {
      case Some(Entity(entity)) => entity
      case None => throw new MnesiaException("Could not find entity by primary key <" + pk + ">")  
    }
  }

  // FIXME: should be able to look up column from index and not have to specific both
  def findByIndex(index: Index, column: Column): Option[AnyRef] = {
    val result: Option[Entity] = mnesia !!! FindByIndex(index, column) 
    result match {
      case Some(Entity(entity)) => entity
      case None => throw new MnesiaException("Could not find entity by index <" + index + ">")  
    }
 } 
 
  def findAll(table: Class[_]): List[AnyRef] = {
    val result: Option[Entities] = mnesia !!! FindAll(table) 
    result match {
      case Some(Entities(entities)) => entities
      case None => throw new MnesiaException("Could not find all entities for table <" + table.getName + ">")  
    }
  }

  def clear = {
    val result: MnesiaMessage = mnesia !? Clear
    result match { 
      case Failure(_, Some(cause)) => throw cause
      case _ => {}
    }
  }

  def changeSchema(schema: String) = mnesia ! ChangeSchema(schema)

  val MNESIA_SERVER_NAME = "mnesia"
  private val mnesia = new GenericServerContainer(MNESIA_SERVER_NAME, () => new Mnesia)
  mnesia.setTimeout(1000)
  private val supervisor = supervisorFactory.newSupervisor

  private object supervisorFactory extends SupervisorFactory {
    override def getSupervisorConfig: SupervisorConfig = {
      SupervisorConfig(
        RestartStrategy(OneForOne, 5, 1000),
        Worker(
          mnesia,
          LifeCycle(Permanent, 100))
        :: Nil)
    }
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
// FIXME: should not be able to be created outside the mnesia package  
sealed case class PK(value: Long, table: Class[_]) extends Ordered[PK] {
  def compare(that: PK): Int = this.value.toInt - that.value.toInt
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
case class Table(clazz: Class[_], columns: List[Column], var indices: List[Column]) {
  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, clazz)
    result = HashCode.hash(result, columns)
    result
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
case class Column(name: String, table: Class[_]) {
  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, name)
    result = HashCode.hash(result, table)
    result
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
private[mnesia] class Mnesia extends GenericServer with Logging {
  val DEFAULT_SCHEMA = "default"

  private val TREAP_CLASS = Class.forName("scala.actors.mnesia.Treap")
  private val METHOD_TREAP_UPD = TREAP_CLASS.getDeclaredMethod("upd", Array(classOf[AnyRef], classOf[AnyRef]))

  private var currentSchema = DEFAULT_SCHEMA

  // FIXME: update to ConcurrentHashMaps
  private val schema =      Map[Class[_]  , Table]()
  private val db =          Map[Column, Treap[PK, AnyRef]]()
  private val indices =     Map[Column, Treap[Index, AnyRef]]()
  private val identityMap = Map[AnyRef, PK]()
  private val dbLock =      new ReadWriteLock
  private val indexLock =   new ReadWriteLock  

  override def body: PartialFunction[Any, Unit] = {
    case CreateTable(table) => 
      log.info("Creating table <{}>", table)
      createTable(table)
    
    case AddIndex(name, table, indexFactory) => 
      log.info("Adding index <{}@{}>", name, table.getName)
      addIndex(name, table, indexFactory)
    
    case Store(entity) => 
      log.debug("Storing entity <{}>", entity)
      reply(PrimaryKey(store(entity)))
    
    case RemoveByEntity(entity) => 
      log.debug("Removing entity <{}>", entity)
      remove(entity)
        
    case RemoveByPK(pk) => 
      log.debug("Removing entity with primary key <{}>", pk)
      remove(pk)
    
    case FindByPK(pk) => 
      log.debug("Finding entity with primary key <{}>", pk)
      reply(Entity(findByPK(pk)))    
      
    case FindByIndex(index, column) => 
      log.debug("Finding entity by its index <{}>", index)
      reply(Entity(findByIndex(index, column)))    
    
    case FindAll(table) => 
      log.debug("Finding all entities in table <{}>", table.getName)
      reply(Entities(findAll(table)))    
    
    case ChangeSchema(schema) => 
      log.info("Changing schema to <{}>", schema)
      changeSchema(schema)
          
    case Clear => 
      log.info("Clearing and reinitializes the database for schema <{}>", currentSchema)
      try { 
        clear 
        log.info("Database cleared successfully")
        reply(Success)
      } catch { case e => 
        log.error("Database could not be cleared due to: " + e.getMessage) 
        reply(Failure(e.getMessage, Some(e)))
      }
  }

  def createTable(table: Class[_]) = {
    val columns: List[Column] = for (field <- table.getDeclaredFields.toList if field.getName != "$outer") yield Column(field.getName, field.getType)
    log.info("Creating table <{}> with columns <{}>", table.getName, columns)

    if (schema.contains(table)) throw new IllegalArgumentException("Table <" + table.getName + "> already exists")
    schema += table -> Table(table, columns, Nil)

    dbLock.withWriteLock {
      PrimaryKeyFactory.init(table)
      db += PrimaryKeyFactory.indexFor(table) -> new Treap[PK, AnyRef]
    }
  }

  def addIndex[I](name: String, table: Class[_], indexFactory: (I) => Index) = dbLock.withWriteLock { indexLock.withWriteLock {
    val field =
      try { table.getDeclaredField(name) }
      catch { case e => throw new IllegalArgumentException("Could not create index <" + name + "> for table <" + table.getName + "> due to: " + e.toString) }
    field.setAccessible(true)

    val currentDB = db(PrimaryKeyFactory.indexFor(table))
    val emptyTreap = new Treap[Index, AnyRef]()
    val fullTreap = currentDB.elements.foldLeft(emptyTreap) { (treap, entry) => {
      val entity = entry._2
      val index = indexFactory(field.get(entity).asInstanceOf[I])
      treap.upd(index, entity)
    }}
    
    // add index to index table
    val column = Column(name, table)
    indices += column -> fullTreap
    
    // add index to schema
    val schemaTable = schema(table)
    schemaTable.indices = column :: schemaTable.indices
    log.info("Index <{}> for table <{}> has been compiled.", name, table.getName)
  }}

  def store(entity: AnyRef): PK = dbLock.withWriteLock {
    val table = entity.getClass
    ensureTableExists(table)

    val pk = if (identityMap.contains(entity)) identityMap(entity)
             else {
               val pk = PrimaryKeyFactory.next(table)
               identityMap += entity -> pk
               pk
             }

    val pkIndex = PrimaryKeyFactory.indexFor(table)
    db(pkIndex) = db(pkIndex).upd(pk, entity)
      
/*
    // ===============================================================================
    // FIXME: I need to get this index somehow -- calculated in the addIndex method    
    val index = indexFactory(field.get(entity).asInstanceOf[I])
    // ===============================================================================

    // update indices
    schema(table).indices.foreach(column => indices(column).upd(index, entity))
*/
    pk
  }

  def remove(entity: AnyRef) = dbLock.withWriteLock { indexLock.withWriteLock {
    val table = entity.getClass
    ensureTableExists(table)
    ensureEntityExists(entity)

    val index = PrimaryKeyFactory.indexFor(table)
    val pk = identityMap(entity)
    val newTreap = db(index).del(pk)

    db(index) = newTreap
    identityMap - entity

    // FIXME: remove entity from all (0..N) indices it is stored in
  }}

  def remove(pk: PK) = dbLock.withWriteLock { indexLock.withWriteLock {
    ensureTableExists(pk.table)
 
    val index = PrimaryKeyFactory.indexFor(pk.table)
    val entity = db(index).get(pk)
    val newTreap = db(index).del(pk)

    db(index) = newTreap
    identityMap - entity

    // FIXME: remove entity from all (0..N) indices it is stored in
  }}

  def findByPK(pk: PK): Option[AnyRef] = dbLock.withReadLock {
    ensureTableExists(pk.table)
    db(PrimaryKeyFactory.indexFor(pk.table)).get(pk)
  }

  // FIXME: should be able to look up column from index and not have to specific both
  def findByIndex(index: Index, column: Column): Option[AnyRef] = indexLock.withReadLock {
    ensureTableExists(column.table)
    ensureIndexExists(column)
    indices(column).get(index)
  }

  def findAll(table: Class[_]): List[AnyRef] = dbLock.withReadLock {
    ensureTableExists(table)
    db(PrimaryKeyFactory.indexFor(table)).elements.toList
  }

  def clear = dbLock.withWriteLock { indexLock.withWriteLock {
    db.clear
    identityMap.clear
    indices.clear
    for (table <- schema.values) db += PrimaryKeyFactory.indexFor(table.clazz) -> new Treap[PK, AnyRef]
  }}

  def changeSchema(schema: String) = {
    currentSchema = schema
    // FIXME: implement support for multiple schemas
  }

  private def ensureTableExists(table: Class[_]) = dbLock.withReadLock {
    if (!db.contains(Column(PrimaryKeyFactory.name, table))) throw new IllegalArgumentException("Table [" + table.getName + "] does not exist")
  }

  private def ensureIndexExists(index: Column) = indexLock.withReadLock {
    if (!indices.contains(index)) throw new IllegalArgumentException("Index [" + index + "] does not exist")
  }

  private def ensureEntityExists(entity: AnyRef) = dbLock.withReadLock {
    if (!identityMap.contains(entity)) throw new IllegalArgumentException("Entity [" + entity + "] is not managed")
  }

  /**
   * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
   */
  private object PrimaryKeyFactory {
    val name = "PK"
    private val keys = Map[Class[T] forSome {type T}, AtomicLong]()

    def init(table: Class[_]) = dbLock.withReadLock {
      val currrentIndex =
        if (db.contains(PrimaryKeyFactory.indexFor(table))) {
          val treap = db(PrimaryKeyFactory.indexFor(table))
          if (!treap.isEmpty) treap.lastKey.value.asInstanceOf[Long] else 1L
        } else 1L
      keys += table -> new AtomicLong(currrentIndex)
   }

    def next(table: Class[_]): PK = {
      if (!keys.contains(table)) throw new IllegalStateException("Primary key generator for table <" + table.getName + "> has not been initialized")
      PK(keys(table).getAndIncrement, table)
    }

    def indexFor(table: Class[_]) = Column(name, table)
  }
}
