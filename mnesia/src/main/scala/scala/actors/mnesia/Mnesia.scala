/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.mnesia

import scala.actors.Actor._
import scala.actors.behavior._
import scala.actors.behavior.Helpers._

import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.atomic.AtomicLong
import java.lang.reflect.Field

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
@serializable
sealed abstract case class MnesiaMessage

// ------------------------
// --- REQUEST MESSAGES ---
// ------------------------
@serializable
case class CreateTable(table: Class[_]) extends MnesiaMessage
@serializable
case class AddIndex(name: String, table: Class[_], indexFactory: (Any) => Index) extends MnesiaMessage
@serializable
case object Clear extends MnesiaMessage
case class ChangeSchema(schema: String) extends MnesiaMessage

@serializable
case class RemoveByEntity(entity: AnyRef) extends MnesiaMessage
@serializable
case class RemoveByPK(pk: PK) extends MnesiaMessage

/** Reply message is PrimaryKey(..) */
@serializable
case class Store(entity: AnyRef) extends MnesiaMessage

/** Reply message is Entity(..) */
case class FindByPK(pk: PK) extends MnesiaMessage

/** Reply message is Entities(..) */
case class FindByIndex(index: Index, column: Column) extends MnesiaMessage

/** Reply message is Entities(..) */
case class FindAll(table: Class[_]) extends MnesiaMessage

case class Replication(message: MnesiaMessage) extends MnesiaMessage
case class AddReplica(replica: Actor) extends MnesiaMessage

//-----------------------
// --- REPLY MESSAGES ---
//-----------------------
case class PrimaryKey(pk: PK) extends MnesiaMessage
case class Entity(entity: Option[AnyRef]) extends MnesiaMessage
case class Entities(list: List[AnyRef]) extends MnesiaMessage

@serializable
case object Success extends MnesiaMessage
@serializable
case class Failure(message: String, cause: Option[Throwable]) extends MnesiaMessage

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
@serializable
class MnesiaException(val message: String) extends RuntimeException {
  override def getMessage: String = message
  override def toString: String = "scala.actors.mnesia.MnesiaException: " + message
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object Mnesia extends Logging {
  
  val MNESIA_SERVER_NAME = "mnesia"
  val SHUTDOWN_TIME = 1000
  val RESTART_WITHIN_TIME_RANGE = 1000
  val MAX_NUMBER_OF_RESTART_BEFORE_GIVING_UP = 5

  private val mnesia = new GenericServerContainer(MNESIA_SERVER_NAME, () => new Mnesia)
  mnesia.setTimeout(1000)
  
  private object supervisorFactory extends SupervisorFactory {
    override def getSupervisorConfig: SupervisorConfig = {
      SupervisorConfig(
        RestartStrategy(OneForOne, MAX_NUMBER_OF_RESTART_BEFORE_GIVING_UP, RESTART_WITHIN_TIME_RANGE),
        Worker(
          mnesia,
          LifeCycle(Permanent, SHUTDOWN_TIME))
        :: Nil)
    }
  }
  
  private val supervisor = supervisorFactory.newSupervisor

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

  def findByPK[T <: AnyRef](pk: PK): Option[T] = {
    val result: Option[Entity] = mnesia !!! FindByPK(pk) 
    result match {
      case Some(Entity(entity)) => entity.asInstanceOf[Option[T]]
      case None => throw new MnesiaException("Could not find entity by primary key <" + pk + ">")  
    }
  }

  def findByIndex[T <: AnyRef](index: Index, column: Column): List[T] = {
    val result: Option[Entities] = mnesia !!! FindByIndex(index, column) 
    result match {
      case Some(Entities(entities)) => entities.asInstanceOf[List[T]]
      case None => throw new MnesiaException("Could not find any entities by index <" + index + ">")  
    }
 } 
 
  def findAll[T <: AnyRef](table: Class[_]): List[T] = {
    val result: Option[Entities] = mnesia !!! FindAll(table) 
    result match {
      case Some(Entities(entities)) => entities.asInstanceOf[List[T]]
      case None => throw new MnesiaException("Could not find all entities for table <" + table.getName + ">")  
    }
  }

  def clear = {
    val result: MnesiaMessage = mnesia !!! (Clear, throw new MnesiaException("Timed out while trying to clear the database"), 10000)
    result match { 
      case Failure(_, Some(cause)) => throw cause
      case _ => {}
    }
  }

  def addReplica(replica: Actor) = mnesia ! AddReplica(replica)

  def changeSchema(schemaName: String) = mnesia ! ChangeSchema(schemaName)

  def init(schema: AnyRef) = mnesia.init(schema)
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
@serializable
abstract case class PK extends Ordered[PK] {
  val value: Long
  val table: Class[_]
  def compare(that: PK): Int = this.value.toInt - that.value.toInt
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
@serializable
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
@serializable
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
  private var schema =      Map[Class[_], Table]()
  private val db =          Map[Column, Treap[PK, AnyRef]]()
  private val indices =     Map[Column, Treap[Index, Map[PK, AnyRef]]]()
  private val identityMap = Map[AnyRef, PK]()
  private val replicas =    new ArrayBuffer[Actor]
  private val dbLock =      new ReadWriteLock
  private val indexLock =   new ReadWriteLock  

  override def body: PartialFunction[Any, Unit] = {
    case AddReplica(replica: Actor) => 
      log.debug("Adding a new replica <{}>", replica)
      replicas += replica
      
    case Replication(message: MnesiaMessage) => 
      try { 
        log.debug("Received replication message <{}>", message)
        Actor.self.send(message, this)
        reply(Success)
      } catch { case e => 
        log.error("Replication message could not be processed due to: {}", e.getMessage) 
        reply(Failure(e.getMessage, Some(e)))
      }
      
    case CreateTable(table) => 
      log.info("Creating table <{}>", table)
      try { 
        createTable(table) 
      } catch { 
        case e => log.error("Could not create table due to: {}", e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
      replicate(CreateTable(table))  
      
    case AddIndex(name, table, indexFactory) => 
      log.info("Adding index <{}@{}>", name, table.getName)
      try {
        addIndex(name, table, indexFactory)
      } catch { 
        case e => log.error("Could not create index due to: {}", e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
      replicate(AddIndex(name, table, indexFactory))
          
    case Store(entity) => 
      log.debug("Storing entity <{}>", entity)
      try {  
        reply(PrimaryKey(store(entity)))
      } catch { 
        case e => log.error("Could not store entity <{}> due to: {}", entity, e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
      replicate(Store(entity))
          
    case RemoveByEntity(entity) => 
      log.debug("Removing entity <{}>", entity)
      try {  
        remove(entity)
      } catch { 
        case e => log.error("Could not remove entity <{}> due to: {}", entity, e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
      replicate(RemoveByEntity(entity))
        
    case RemoveByPK(pk) => 
      log.debug("Removing entity with primary key <{}>", pk)
      try {
        remove(pk)
      } catch { 
        case e => log.error("Could not remove entity with primary key <{}> due to: {}", pk, e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
      replicate(RemoveByPK(pk))
    
    case FindByPK(pk) => 
      log.debug("Finding entity with primary key <{}>", pk)
      try {
        reply(Entity(findByPK(pk)))    
      } catch { 
        case e => log.error("Could not find entity by primary key <{}> due to: {}", pk, e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
      
    case FindByIndex(index, column) => 
      log.debug("Finding entity by its index <{}>", index)
      try {
        reply(Entities(findByIndex(index, column)))    
      } catch { 
        case e => log.error("Could not find entity by index <{}> due to: {}", index, e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
    
    case FindAll(table) => 
      log.debug("Finding all entities in table <{}>", table.getName)
      try {  
        reply(Entities(findAll(table)))    
      } catch { 
        case e => log.error("Could not find all entities in table <{}> due to: {}", table.getName, e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e 
      }
    
    case ChangeSchema(schema) => 
      log.info("Changing schema to <{}>", schema)
      try {  
        changeSchema(schema)
      } catch { 
        case e => log.error("Could not change schema <{}> due to: {}", schema, e.getMessage)
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
        log.error("Database could not be cleared due to: {}", e.getMessage) 
        reply(Failure(e.getMessage, Some(e)))
      }
      replicate(Clear)
  }

  override def reinit(config: AnyRef) {
    (new Exception("bla")).printStackTrace
    log.info("Database schema <{}> successfully reinitialized after server crash", currentSchema)
    schema = config.asInstanceOf[Map[Class[_], Table]]
    
    // TODO: read in replicated state after server crash
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
    Mnesia.init(schema) // save latest schema in server container
  }

  def addIndex(name: String, table: Class[_], indexFactory: (Any) => Index) = 
    dbLock.withWriteLock { indexLock.withWriteLock {
    val field = try { table.getDeclaredField(name) } catch { case e => throw new IllegalArgumentException("Could not create index <" + name + "> for table <" + table.getName + "> due to: " + e.toString) }
    field.setAccessible(true)

    val currentDB = db(PK.getPrimaryKeyColumnFor(table))
    val emptyTreap = new Treap[Index, Map[PK, AnyRef]]()

    val fullTreap = currentDB.elements.foldLeft(emptyTreap) { (indexMap, entry) => {
      val entity = entry._2
      val index = createIndexFor(field, entity, indexFactory)
      indexMap.get(index) match {
        case Some(entities) => 
          entities += getPKFor(entity) -> entity
          indexMap
        case None => 
          indexMap.upd(index, Map[PK, AnyRef](getPKFor(entity) -> entity))
      }
    }}
    
    val column = Column(name, table)
    
    // add index to index table
    indices += column -> fullTreap
    
    // add index to schema
    val schemaTable = schema(table)
    schemaTable.indices = (column, field, indexFactory) :: schemaTable.indices
    
    Mnesia.init(schema) // save latest schema in server container
    log.info("Index <{}> for table <{}> has been compiled", name, table.getName)
  }}

  def store(entity: AnyRef): PK = dbLock.withWriteLock {
    val table = entity.getClass
    ensureTableExists(table)

    val pk = getPKFor(entity)

    val pkIndex = PK.getPrimaryKeyColumnFor(table)
    db(pkIndex) = db(pkIndex).upd(pk, entity)

    // update indices
    schema(table).indices.foreach { item =>
      val (column, field, indexFactory) = item 
      val indexMap = indices(column)
      val index = createIndexFor(field, entity, indexFactory)
      indexMap.get(index) match {
        case Some(entities: Map[PK, AnyRef]) => 
          entities += getPKFor(entity) -> entity
        case None => 
          addIndex(column.name, column.table, indexFactory)
          indexMap.upd(index, Map[PK, AnyRef](getPKFor(entity) -> entity))
      }
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

  def findByIndex(index: Index, column: Column): List[AnyRef] = indexLock.withReadLock {
    ensureTableExists(column.table)
    ensureIndexExists(column)
    indices(column).get(index).getOrElse(throw new IllegalStateException("No such index <" + index + "> for column <" + column + ">")).values.toList
  }

  def findAll(table: Class[_]): List[AnyRef] = dbLock.withReadLock {
    ensureTableExists(table)
    db(PK.getPrimaryKeyColumnFor(table)).values.toList
  }

  def clear = dbLock.withWriteLock { indexLock.withWriteLock {
    db.clear
    identityMap.clear
    indices.clear
    for (table <- schema.values) db += PK.getPrimaryKeyColumnFor(table.clazz) -> new Treap[PK, AnyRef]    
    for { 
      table <- schema.values
      (column, field, indexFactory) <- table.indices
    } {
      indices += column -> new Treap[Index, Map[PK, AnyRef]]()
    }
  }}

  def changeSchema(schema: String) = {
    currentSchema = schema
    // FIXME: implement support for multiple schemas
  }

  def replicate(message: MnesiaMessage) = 
    for (replica <- replicas) {
      // FIXME: change replication to async (just lazy now)
      replica !? Replication(message) match {
        case Success => log.debug("Replication message <{}> successfully delivered", message) 
        case Failure => log.error("Replication message <{}> failed to be delivered", message)
          // TODO: retry etc.
      }
    }
   
  private def createIndexFor(field: Field, entity: AnyRef, indexFactory: (Any) => Index): Index = 
    indexFactory(field.get(entity))
  
  // FIXME: should use reflection to retrieve the id field in the entity (dump the identityMap)
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

  @serializable
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
