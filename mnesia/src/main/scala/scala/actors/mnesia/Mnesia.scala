/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.mnesia

import scala.actors.Actor._
import scala.actors.behavior._
import scala.actors.behavior.Helpers._
import scala.collection.mutable.Map

import java.util.concurrent.atomic.AtomicLong
import java.lang.reflect.Field

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
sealed abstract case class MnesiaMessage

// ------------------------
// --- REQUEST MESSAGES ---
// ------------------------
case class CreateTable(table: Class[_]) extends MnesiaMessage
case class AddIndex(name: String, table: Class[_], indexFactory: (Any) => Index) extends MnesiaMessage
case class ChangeSchema(schema: String) extends MnesiaMessage
case object Clear extends MnesiaMessage

case class RemoveByEntity(entity: AnyRef) extends MnesiaMessage
case class RemoveByPK(pk: PK) extends MnesiaMessage

/** Reply message is PrimaryKey(..) */
case class Store(entity: AnyRef) extends MnesiaMessage
/** Reply message is Entity(..) */
case class FindByPK(pk: PK) extends MnesiaMessage
/** Reply message is Entity(..) */
case class FindByIndex(index: Index, column: Column) extends MnesiaMessage
/** Reply message is Entities(..) */
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
  override def getMessage: String = message
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

  def addIndex(name: String, table: Class[_], indexFactory: (Any) => Index) = 
    mnesia ! AddIndex(name, table, indexFactory)

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
abstract case class PK extends Ordered[PK] {
  val value: Long
  val table: Class[_]
  def compare(that: PK): Int = this.value.toInt - that.value.toInt
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
case class Table(clazz: Class[_], columns: List[Column], var indices: List[Tuple3[Column, Field, (Any) => Index]]) {
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
      try { 
        createTable(table) 
      } catch { 
        case e => log.error("Could not create table due to: " + e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
    
    case AddIndex(name, table, indexFactory) => 
      log.info("Adding index <{}@{}>", name, table.getName)
      try {
        addIndex(name, table, indexFactory)
      } catch { 
        case e => log.error("Could not create index due to: " + e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
    
    case Store(entity) => 
      log.debug("Storing entity <{}>", entity)
      try {  
        reply(PrimaryKey(store(entity)))
      } catch { 
        case e => log.error("Could not store entity <" + entity + "> due to: " + e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
    
    case RemoveByEntity(entity) => 
      log.debug("Removing entity <{}>", entity)
      try {  
        remove(entity)
      } catch { 
        case e => log.error("Could not remove entity <" + entity + "> due to: " + e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
        
    case RemoveByPK(pk) => 
      log.debug("Removing entity with primary key <{}>", pk)
      try {
        remove(pk)
      } catch { 
        case e => log.error("Could not remove entity <" + pk + "> due to: " + e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
    
    case FindByPK(pk) => 
      log.debug("Finding entity with primary key <{}>", pk)
      try {
        reply(Entity(findByPK(pk)))    
      } catch { 
        case e => log.error("Could not find entity by primary key <" + pk + "> due to: " + e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
      
    case FindByIndex(index, column) => 
      log.debug("Finding entity by its index <{}>", index)
      try {
        reply(Entity(findByIndex(index, column)))    
      } catch { 
        case e => log.error("Could not find entity by index <" + index + "> due to: " + e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
    
    case FindAll(table) => 
      log.debug("Finding all entities in table <{}>", table.getName)
      try {  
        reply(Entities(findAll(table)))    
      } catch { 
        case e => log.error("Could not find all entities in table <" + table.getName + "> due to: " + e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
    
    case ChangeSchema(schema) => 
      log.info("Changing schema to <{}>", schema)
      try {  
        changeSchema(schema)
      } catch { 
        case e => log.error("Could not change schema <" + schema + "> due to: " + e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
          
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
    val columns: List[Column] = for (field <- table.getDeclaredFields.toList if field.getName != "$outer") 
                                yield Column(field.getName, field.getType)
    log.info("Creating table <{}> with columns <{}>", table.getName, columns)

    if (schema.contains(table)) throw new IllegalArgumentException("Table <" + table.getName + "> already exists")
    schema += table -> Table(table, columns, Nil)

    dbLock.withWriteLock {
      PK.init(table)
      db += PK.getPrimaryKeyColumnFor(table) -> new Treap[PK, AnyRef]
    }
  }

  def addIndex(name: String, table: Class[_], indexFactory: (Any) => Index) = 
    dbLock.withWriteLock { indexLock.withWriteLock {
    val field = try { table.getDeclaredField(name) } catch { case e => throw new IllegalArgumentException("Could not create index <" + name + "> for table <" + table.getName + "> due to: " + e.toString) }
    field.setAccessible(true)

    val currentDB = db(PK.getPrimaryKeyColumnFor(table))
    val emptyTreap = new Treap[Index, AnyRef]()

    val fullTreap = currentDB.elements.foldLeft(emptyTreap) { (treap, entry) => {
      val entity = entry._2
      val index = createIndexFor(field, entity, indexFactory)
      treap.upd(index, entity)
    }}
    
    val column = Column(name, table)
    
    // add index to index table
    indices += column -> fullTreap
    
    // add index to schema
    val schemaTable = schema(table)
    schemaTable.indices = (column, field, indexFactory) :: schemaTable.indices
    
    log.info("Index <{}> for table <{}> has been compiled", name, table.getName)
  }}

  def store(entity: AnyRef): PK = dbLock.withWriteLock {
      val table = entity.getClass
    ensureTableExists(table)

    val pk = getPKFor(entity)

    val pkIndex = PK.getPrimaryKeyColumnFor(table)
    db(pkIndex) = db(pkIndex).upd(pk, entity)

    // update indices
    // FIXME: add list entries to cover multiple index with same hash (eg same value)
    schema(table).indices.foreach { item =>
      val (column, field, indexFactory) = item 
      val index = createIndexFor(field, entity, indexFactory)
      indices(column).upd(index, entity)
    }
    pk
  }

  def remove(entity: AnyRef) = dbLock.withWriteLock { indexLock.withWriteLock {
    val table = entity.getClass
    ensureTableExists(table)
    ensureEntityExists(entity)

    val index = PK.getPrimaryKeyColumnFor(table)
    val pk = identityMap(entity)
    val newTreap = db(index).del(pk)

    db(index) = newTreap
    identityMap - entity

    // FIXME: remove entity from all (0..N) indices it is stored in
  }}

  def remove(pk: PK) = dbLock.withWriteLock { indexLock.withWriteLock {
    ensureTableExists(pk.table)
 
    val index = PK.getPrimaryKeyColumnFor(pk.table)
    val entity = db(index).get(pk)
    val newTreap = db(index).del(pk)

    db(index) = newTreap
    identityMap - entity

    // FIXME: remove entity from all (0..N) indices it is stored in
  }}

  def findByPK(pk: PK): Option[AnyRef] = dbLock.withReadLock {
    ensureTableExists(pk.table)
    db(PK.getPrimaryKeyColumnFor(pk.table)).get(pk)
  }

  // FIXME: should be able to look up column from index and not have to specific both
  // FIXME: need to return a List in case we have more than one match
  def findByIndex(index: Index, column: Column): Option[AnyRef] = indexLock.withReadLock {
    ensureTableExists(column.table)
    ensureIndexExists(column)
    indices(column).get(index)
  }

  def findAll(table: Class[_]): List[AnyRef] = dbLock.withReadLock {
    ensureTableExists(table)
    db(PK.getPrimaryKeyColumnFor(table)).elements.toList
  }

  def clear = dbLock.withWriteLock { indexLock.withWriteLock {
    db.clear
    identityMap.clear
    indices.clear
    for (table <- schema.values) db += PK.getPrimaryKeyColumnFor(table.clazz) -> new Treap[PK, AnyRef]    
    for { 
      table <- schema.values
      (column, _, _) <- table.indices
    } indices += column -> new Treap[Index, AnyRef]()
  }}

  def changeSchema(schema: String) = {
    currentSchema = schema
    // FIXME: implement support for multiple schemas
  }

  private def createIndexFor(field: Field, entity: AnyRef, indexFactory: (Any) => Index): Index = 
    indexFactory(field.get(entity))
  
  private def getPKFor(entity: AnyRef): PK = {
    if (identityMap.contains(entity)) identityMap(entity)
    else {
      val pk = PK.next(entity.getClass)
      identityMap += entity -> pk
      pk
    }
  }
  
  private def ensureTableExists(table: Class[_]) = dbLock.withReadLock {
    if (!db.contains(Column(PK.name, table))) throw new IllegalArgumentException("Table [" + table.getName + "] does not exist")
  }

  private def ensureIndexExists(index: Column) = indexLock.withReadLock {
    if (!indices.contains(index)) throw new IllegalArgumentException("Index [" + index + "] does not exist")
  }

  private def ensureEntityExists(entity: AnyRef) = dbLock.withReadLock {
    if (!identityMap.contains(entity)) throw new IllegalArgumentException("Entity [" + entity + "] is not managed")
  }

  case class PKImpl private[Mnesia] (override val value: Long, override val table: Class[_]) extends PK

  /**
   * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
   */
  private[this] object PK {
    val name = "PK"
    val keys = Map[Class[T] forSome {type T}, AtomicLong]()

    def init(table: Class[_]) = dbLock.withReadLock {
      val currrentIndex =
        if (db.contains(getPrimaryKeyColumnFor(table))) {
          val treap = db(getPrimaryKeyColumnFor(table))
          if (!treap.isEmpty) treap.lastKey.value.asInstanceOf[Long] else 1L
        } else 1L
      keys += table -> new AtomicLong(currrentIndex)
   }

    def next(table: Class[_]): PK = {
      if (!keys.contains(table)) throw new IllegalStateException("Primary key generator for table <" + table.getName + "> has not been initialized")
      PKImpl(keys(table).getAndIncrement, table)
    }

    def getPrimaryKeyColumnFor(table: Class[_]) = Column(name, table)
  }
}
