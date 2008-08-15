/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.mnesia

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
@serializable
sealed abstract case class DBMessage

// -----------------------
// --- CONFIG MESSAGES ---
// -----------------------
@serializable
case class Config(storageStrategy: StorageStrategy) {
  val schema = Map[Class[_], Table]()
}
@serializable
abstract case class StorageStrategy extends DBMessage 
@serializable
case class EvictableStorageStrategy(timeout: Long) extends StorageStrategy
@serializable
case object InMemoryStorageStrategy extends StorageStrategy
@serializable
case object DiskBasedStorageStrategy extends StorageStrategy
@serializable
case object ReplicatedAtomicStorageStrategy extends StorageStrategy
@serializable
case object ReplicatedEventuallyConsistentStorageStrategy extends StorageStrategy

// ----------------------------------------------
// --- PRIMARY KEY GENERATION SCHEME MESSAGES ---
// ----------------------------------------------
@serializable
abstract case class PrimaryKeySequenceScheme extends DBMessage 
@serializable
case class GeneratePrimaryKeySequence extends PrimaryKeySequenceScheme
@serializable
case class CustomPrimaryKeySequence extends PrimaryKeySequenceScheme


// ------------------------
// --- REQUEST MESSAGES ---
// ------------------------
@serializable
case class CreateTableWithGeneratedPK(table: Class[_], pkName: String) extends DBMessage
@serializable
case class CreateTableWithCustomPK(table: Class[_], pkName: String, pkIndexFactory: (Any) => Index) extends DBMessage 

@serializable
case class AddIndex(name: String, table: Class[_], indexFactory: (Any) => Index) extends DBMessage
@serializable
case object Clear extends DBMessage

@serializable
case class RemoveByEntity(entity: AnyRef) extends DBMessage
@serializable
case class RemoveByPK(pkName: Index, table: Class[_]) extends DBMessage
@serializable
case class RemoveByIndex(pkName: Index, columnName: String, table: Class[_]) extends DBMessage

/** Reply message is PrimaryKey(..) */
@serializable
case class Store(entity: AnyRef) extends DBMessage

/** Reply message is Entity(..) */
@serializable
case class FindByPK(index: Index, table: Class[_]) extends DBMessage
/** Reply message is Entities(..) */
@serializable
case class FindByIndex(index: Index, columnName: String, table: Class[_]) extends DBMessage
/** Reply message is Entities(..) */
@serializable
case class FindAll(table: Class[_]) extends DBMessage

//-----------------------
// --- REPLY MESSAGES ---
//-----------------------
@serializable
case class PrimaryKey(pk: Index) extends DBMessage
@serializable
case class Entity(entity: Option[AnyRef]) extends DBMessage
@serializable
case class Entities(list: List[AnyRef]) extends DBMessage

@serializable
case object Success extends DBMessage
@serializable
case class Failure(message: String, cause: Option[Throwable]) extends DBMessage

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
@serializable
class DBException(val message: String) extends RuntimeException {
  override def getMessage: String = message
  override def toString: String = getClass.getName + ": " + message
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
@serializable
case class Table(
  clazz: Class[_], 
  columns: List[Column], 
  pk: Column,  
  pkIndexFactory: (Any) => Index, 
  pkSequenceScheme: PrimaryKeySequenceScheme,
  var indices: List[Tuple3[Column, Field, (Any) => Index]]) {
  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, clazz)
    result = HashCode.hash(result, pk)
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
object DB extends Logging {

  val SERVER_NAME = "GPS_SERVER_CACHE"
  val SHUTDOWN_TIME = 1000
  val RESTART_WITHIN_TIME_RANGE = 1000
  val MAX_NUMBER_OF_RESTART_BEFORE_GIVING_UP = 5

  private val isInitialized = new AtomicBoolean(false)
  private val isStarted = new AtomicBoolean(false)

  private val db = new GenericServerContainer(SERVER_NAME, () => new DB)
  db.setTimeout(1000)

  private object supervisorFactory extends SupervisorFactory {
    override def getSupervisorConfig: SupervisorConfig = {
      SupervisorConfig(
        RestartStrategy(OneForOne, MAX_NUMBER_OF_RESTART_BEFORE_GIVING_UP, RESTART_WITHIN_TIME_RANGE),
        Worker(
          db,
          LifeCycle(Permanent, SHUTDOWN_TIME))
        :: Nil)
    }
  }

  private val supervisor = supervisorFactory.newSupervisor

  def init(config: Config): DB.type =  {
    if (!isInitialized.getAndSet(true)) db.init(config)
    this
  }
  
  def start: DB.type = {
    if (!isInitialized.get) throw new IllegalStateException("DB is not initialized, make sure that the <DB.init(config)> is invoked prior to <DB.start>")
    if (!isStarted.getAndSet(true)) supervisor ! Start
    this
  }
  
  def stop = {
    if (!isStarted.get) throw new IllegalStateException("DB is not started")
    if (isStarted.getAndSet(false)) supervisor ! Stop
  }

  def createTable(table: Class[_], pkName: String): DB.type = {
    db ! CreateTableWithGeneratedPK(table, pkName)
    this
  }

  def createTable(table: Class[_], pkName: String, pkIndexFactory: (Any) => Index): DB.type = {
    db ! CreateTableWithCustomPK(table, pkName, pkIndexFactory)
    this
  }

  def addIndex(columnName: String, table: Class[_], indexFactory: (Any) => Index): DB.type = {
    db ! AddIndex(columnName, table, indexFactory)
    this
  }

  def store(entity: AnyRef): Index = {
    val result: Option[PrimaryKey] = db !!! Store(entity)
    result match {
      case Some(PrimaryKey(pkIndex)) => pkIndex
      case None => throw new DBException("Could not store entity <" + entity + ">")
    }
  }

  def remove(entity: AnyRef) = db ! RemoveByEntity(entity)

  def remove(pkIndex: Index, table: Class[_]) = db ! RemoveByPK(pkIndex, table)

  def findByPK[T <: AnyRef](index: Index, table: Class[_]): Option[T] = {
    val result: Option[Entity] = db !!! FindByPK(index, table)
    result match {
      case Some(Entity(entity)) => entity.asInstanceOf[Option[T]]
      case None => None
    }
 }

  def findByIndex[T <: AnyRef](index: Index, columnName: String, table: Class[_]): List[T] = {
    val result: Option[Entities] = db !!! FindByIndex(index, columnName, table)
    result match {
      case Some(Entities(entities)) => entities.asInstanceOf[List[T]]
      case None => List[T]()
    }
 }

  def findAll[T <: AnyRef](table: Class[_]): List[T] = {
    val result: Option[Entities] = db !!! FindAll(table)
    result match {
      case Some(Entities(entities)) => entities.asInstanceOf[List[T]]
      case None => List[T]()
    }
  }

  def clear = {
    val result: DBMessage = db !!! (Clear, throw new DBException("Timed out while trying to clear the database"), 10000)
    result match {
      case Failure(_, Some(cause)) => throw cause
      case _ => {}
    }
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
private[mnesia] class DB extends GenericServer with Logging {

  private var storage: Storage = _

  override def init(config: AnyRef) = {
    val cfg = config.asInstanceOf[Config]
    storage = createStorage(cfg.storageStrategy, cfg.schema)
    log.info("Database initialized: schema <{}> - storage strategy <{}>", cfg.schema, cfg.storageStrategy)
  }

  override def reinit(config: AnyRef) = {
    val cfg = config.asInstanceOf[Config]
    storage = createStorage(cfg.storageStrategy, cfg.schema)
    log.info("Database reinitialized after server crash: schema <{}> - storage strategy <{}>", cfg.schema, cfg.storageStrategy)

    // TODO: reinit DB
  }

  override def body: PartialFunction[Any, Unit] = {
    
    case CreateTableWithGeneratedPK(table, pkName) =>
      log.info("Creating table <{}>", table)
      try {
        storage.createTable(table, pkName)
      } catch {
        case e => log.error("Could not create table due to: {}", e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e
      }

    case CreateTableWithCustomPK(table, pkName, pkIndexFactory) =>
      log.info("Creating table <{}>", table)
      try {
        storage.createTable(table, pkName, pkIndexFactory)
      } catch {
        case e => log.error("Could not create table due to: {}", e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e
      }

    case AddIndex(columnName, table, indexFactory) =>
      log.info("Adding index <{}@{}>", columnName, table.getName)
      try {
        storage.addIndex(columnName, table, indexFactory)
      } catch {
        case e => log.error("Could not create index due to: {}", e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e
      }

    case Store(entity) =>
      log.debug("Storing entity <{}>", entity)
      try {
        reply(PrimaryKey(storage.store(entity)))
      } catch {
        case e => log.error("Could not store entity <{}> due to: {}", entity, e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e
      }

    case RemoveByEntity(entity) =>
      log.debug("Removing entity <{}>", entity)
      try {
        storage.remove(entity)
      } catch {
        case e => log.error("Could not remove entity <{}> due to: {}", entity, e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e
      }

    case RemoveByPK(pkIndex, table) =>
      log.debug("Removing entity with primary key <{}> and table <{}>", pkIndex, table)
      try {
        storage.removeByPK(pkIndex, table)
      } catch {
        case e => log.error("Could not remove entity with primary key <{}> due to: {}", pkIndex, e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e
      }

    case FindByPK(index, table) =>
      log.debug("Finding entity by primary key <{}> and table <{}>", index, table)
      try {
        reply(Entity(Some(storage.findByPK(index, table))))
      } catch {
        case e => log.error("Could not find entity by primary key <{}> due to: {}", index, e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e
      }

    case FindByIndex(index, columnName, table) =>
      log.debug("Finding entity by index <{}> and table <{}>", columnName, table)
      try {
        reply(Entities(storage.findByIndex(index, columnName, table)))
      } catch {
        case e => log.error("Could not find entity by index <{}> due to: {}", index, e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e
      }

    case FindAll(table) =>
      log.debug("Finding all entities in table <{}>", table.getName)
      try {
        reply(Entities(storage.findAll(table)))
      } catch {
        case e => log.error("Could not find all entities in table <{}> due to: {}", table.getName, e.getMessage)
        e.printStackTrace
        // FIXME: reply with Failure
        throw e
      }

    case Clear =>
      log.info("Clearing and reinitializes the database")
      try {
        storage.clear
        log.info("Database cleared successfully")
        reply(Success)
      } catch { case e =>
        log.error("Database could not be cleared due to: {}", e.getMessage)
        reply(Failure(e.getMessage, Some(e)))
      }
  }

  def createStorage(strategy: StorageStrategy, schema: Map[Class[_], Table]) = strategy match {
    case InMemoryStorageStrategy => new InMemoryStorage(schema)
    case EvictableStorageStrategy(timeout: Long) => new EvictableStorage(schema, timeout)
    case DiskBasedStorageStrategy => throw new UnsupportedOperationException("DiskBasedStorageStrategy is yet to be implemented")
    case ReplicatedAtomicStorageStrategy => throw new UnsupportedOperationException("ReplicatedAtomicStorageStrategy is yet to be implemented")
    case ReplicatedEventuallyConsistentStorageStrategy => throw new UnsupportedOperationException("ReplicatedEventuallyConsistentStorageStrategy is yet to be implemented")
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
private[mnesia] abstract class Storage(protected val schema: Map[Class[_], Table]) {

  // FIXME: update to ConcurrentHashMaps???
  protected val db =          Map[Column, Treap[Index, AnyRef]]()
  protected val indices =     Map[Column, Treap[Index, Map[Index, AnyRef]]]()

  protected val dbLock =      new ReadWriteLock
  protected val indexLock =   new ReadWriteLock

  /**
   * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
   */
  private[mnesia] object PKFactory {
    val keys = Map[Class[T] forSome {type T}, AtomicLong]()
    val indexFactory = (key: Any) => key.asInstanceOf[PK]
    def init(table: Class[_]) = dbLock.withReadLock {
      val currrentIndex =
        if (db.contains(getPKColumnFor(table))) {
          val treap = db(getPKColumnFor(table))
          if (!treap.isEmpty) treap.lastKey.value.asInstanceOf[Long] else 1L
        } else 1L
      keys += table -> new AtomicLong(currrentIndex)
    }

    def next(table: Class[_]): PK = {
      if (!keys.contains(table)) init(table) //throw new IllegalStateException("Primary key generator for table <" + table.getName + "> has not been initialized")
      val pk = PK(keys(table).getAndIncrement)
      pk.table = table
      pk
    }
  }

  def createTable(table: Class[_], pkName: String) = {
    try {
    val pkColumn = Column(pkName, table)

    // FIXME: optimize - now we have to refletive calls to getDeclaredFields
    val columns: List[Column] = for (field <- table.getDeclaredFields.toList if field.getName != "$outer")
                                yield Column(field.getName, field.getType)
    log.info("Creating table <{}> with columns <{}>", table.getName, columns)

    val pkField = table.getDeclaredFields.toList.find(field => field.getName == pkName).getOrElse(throw new IllegalArgumentException("Primary key field <" + pkName + "> does not exist"))
    if (pkField.getType != classOf[PK]) throw new IllegalArgumentException("Primary key field <" + pkName + "> for table <" + table.getName + "> has to be of type <PK>")

    if (schema.contains(table)) throw new IllegalArgumentException("Table <" + table.getName + "> already exists")
      schema += table -> Table(table, columns, pkColumn, PKFactory.indexFactory, GeneratePrimaryKeySequence(), Nil)

    dbLock.withWriteLock { 
      db += getPKColumnFor(table) -> new Treap[Index, AnyRef] 
      addIndex(pkName, table, PKFactory.indexFactory)    
    }
    } catch { case e => e.printStackTrace; e }
  }

  def createTable(table: Class[_], pkName: String, pkIndexFactory: (Any) => Index) = {
    val pkColumn = Column(pkName, table)
    val columns: List[Column] = for (field <- table.getDeclaredFields.toList if field.getName != "$outer")
                                yield Column(field.getName, field.getType)
    log.info("Creating table <{}> with columns <{}>", table.getName, columns)

    if (schema.contains(table)) throw new IllegalArgumentException("Table <" + table.getName + "> already exists")
    schema += table -> Table(table, columns, pkColumn, pkIndexFactory, CustomPrimaryKeySequence(), Nil)

    dbLock.withWriteLock { 
      db += getPKColumnFor(table) -> new Treap[Index, AnyRef] 
      addIndex(pkName, table, pkIndexFactory)    
    }
  }

  def addIndex(columnName: String, table: Class[_], indexFactory: (Any) => Index) = dbLock.withWriteLock { indexLock.withWriteLock {
    val field = try { 
      val field = table.getDeclaredField(columnName) 
      field.setAccessible(true)
      field
    } catch { 
      case e => throw new IllegalArgumentException("Could not get index <" + columnName + "> for table <" + table.getName + "> due to: " + e.toString) 
    }

    val currentDB = db(getPKColumnFor(table))
    val emptyTreap = new Treap[Index, Map[Index, AnyRef]]()

    val fullTreap = currentDB.elements.foldLeft(emptyTreap) { (indexMap, entry) => {
      val entity = entry._2
      val index = indexFactory(field.get(entity))
      indexMap.get(index) match {
        case Some(entities) =>
          entities += getPKIndexFor(entity) -> entity
          indexMap
        case None =>
          indexMap.upd(index, Map[Index, AnyRef](getPKIndexFor(entity) -> entity))
      }
    }}

    val column = Column(columnName, table)

    // add index to index table
    indices += column -> fullTreap

    // add index to schema
    val schemaTable = schema(table)
    schemaTable.indices = (column, field, indexFactory) :: schemaTable.indices

    log.info("Index <{}> for table <{}> has been compiled", columnName, table.getName)
  }}

  def store(entity: AnyRef): Index = dbLock.withWriteLock {
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
      val indexMap = indices(column)
      val index = getIndexFor(entity, column.name)
      indexMap.get(index) match {
        case Some(entities) =>
         entities += getPKIndexFor(entity) -> entity
       case None =>
          addIndex(column.name, column.table, indexFactory)
          indexMap.upd(index, Map[Index, AnyRef](getPKIndexFor(entity) -> entity))
      }
    }
    pkIndex
  }

  def remove(entity: AnyRef) = dbLock.withWriteLock { indexLock.withWriteLock {
    val table = entity.getClass
    ensureTableExists(table)
    ensureEntityExists(entity)

    val pkColumn = getPKColumnFor(table)
    val pkIndex = getPKIndexFor(entity)
    val oldTreap = db(pkColumn)
    val newTreap = oldTreap.del(pkIndex)
    db(pkColumn) = newTreap

    // FIXME: remove entity from all (0..N) indices it is stored in
  }}

  def removeByPK(pkIndex: Index, table: Class[_]) = dbLock.withWriteLock { indexLock.withWriteLock {
    ensureTableExists(table)
    val pkColumn = getPKColumnFor(table)
    if (!db.contains(pkColumn)) throw new IllegalArgumentException("Primary key <" + pkColumn.name + "> for table <" + table.getName + "> does not exist")

    val newTreap = db(pkColumn).del(pkIndex)
    db(pkColumn) = newTreap

    // FIXME: remove entity from all (0..N) indices it is stored in
  }}

  // FIXME: implement
  def removeByIndex(index: Index, columnName: String, table: Class[_]) = {}

  def findByPK(pkIndex: Index, table: Class[_]): AnyRef = {
    ensureTableExists(table)
    val pkColumn = getPKColumnFor(table)
    db(pkColumn).get(pkIndex).getOrElse(throw new IllegalStateException("No such primary key <" + pkIndex + "> for table <" + table.getName + ">"))
  }

  def findByIndex(index: Index, columnName: String, table: Class[_]): List[AnyRef] = indexLock.withReadLock {
    ensureTableExists(table)
    val column = getColumnFor(columnName, table)
    ensureIndexExists(column)
    indices(column).get(index).getOrElse(throw new IllegalStateException("No such index <" + columnName + "> for table <" + table.getName + ">")).values.toList
  }

  def findAll(table: Class[_]): List[AnyRef] = dbLock.withReadLock {
    ensureTableExists(table)
    db(getPKColumnFor(table)).values.toList
  }

  def size(table: Class[_]): Int = db(getPKColumnFor(table)).size

  // =======================

  // FIXME: implement these for Index as well OR get rid of PK and only use Index

//   /**
//    * Returns the last key of the table.
//    */
//   def lastPK(table: Class[_]): Index = {
//     ensureTableExists(table)
//     db(PK.getPrimaryKeyColumnFor(table)).lastKey
//   }

//   /**
//    * Creates a ranged projection of this collection with both a lower-bound and an upper-bound.
//    */
//   def range(from: PK, until: PK): List[AnyRef] = {
//     assert(from.table == until.table)
//     ensureTableExists(from.table)
//     db(PK.getPrimaryKeyColumnFor(from.table)).range(from, until).values.toList
//   }

//   /**
//    * Creates a ranged projection of this collection with no upper-bound.
//    */
//   def from(from: PK) : List[AnyRef] = {
//     ensureTableExists(from.table)
//     db(PK.getPrimaryKeyColumnFor(from.table)).from(from).values.toList
//   }

//   /**
//    * Creates a ranged projection of this collection with no lower-bound.
//    */
//   def until(until: PK): List[AnyRef] = {
//     ensureTableExists(until.table)
//     db(PK.getPrimaryKeyColumnFor(until.table)).until(until).values.toList
//   }

  // FIXME: implement these union/intersect/diff

//   def union(that: Treap[A, B]): Treap[A, B] 
//   def intersect(that: Treap[A, B]): Treap[A, B]
//   def diff(that: Treap[A, B]): Treap[A, B] 

  // =======================

  def clear = dbLock.withWriteLock { indexLock.withWriteLock {
    db.clear
    indices.clear
    for (table <- schema.values) db += getPKColumnFor(table.clazz) -> new Treap[Index, AnyRef]
    for {
      table <- schema.values
      (column, field, indexFactory) <- table.indices
    } indices += column -> new Treap[Index, Map[Index, AnyRef]]()
  }}

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

  // FIXME: store away index field in the schema 
  protected def getIndexFor(entity: AnyRef, columnName: String): Index = dbLock.withReadLock {
    try { 
      val (_, field, indexFactory) = schema(entity.getClass).indices.find(item => item._1.name == columnName).getOrElse(throw new IllegalArgumentException("Could not get primary key <" + columnName + "> for table <" + entity.getClass.getName + ">: no such column"))
      indexFactory(field.get(entity))
    } catch { 
      case e => throw new IllegalArgumentException("Could not get index <" + columnName + "> for table <" + entity.getClass.getName + "> due to: " + e.toString) 
    }
  }

  // FIXME: store away index field in the schema 
  protected def setIndexFor(entity: AnyRef, columnName: String, index: Index) = dbLock.withWriteLock {
    try { 
      val (_, field, _) = schema(entity.getClass).indices.find(item => item._1.name == columnName).getOrElse(throw new IllegalArgumentException("Could not get primary key <" + columnName + "> for table <" + entity.getClass.getName + ">: no such column"))
      field.set(entity, index)
      println("entity: " + entity) 
    } catch { 
      case e => throw new IllegalArgumentException("Could not get primary key <" + columnName + "> for table <" + entity.getClass.getName + "> due to: " + e.toString) 
    }
  }

  protected def ensureTableExists(table: Class[_]) = dbLock.withReadLock {
    if (!db.contains(Column(schema(table).pk.name, table))) throw new IllegalArgumentException("Table [" + table.getName + "] does not exist")
  }

  protected def ensureIndexExists(index: Column) = indexLock.withReadLock {
    if (!indices.contains(index)) throw new IllegalArgumentException("Index [" + index + "] does not exist")
  }

  protected def ensureEntityExists(entity: AnyRef) = dbLock.withReadLock {
    if (!db.contains(getPKColumnFor(entity.getClass))) throw new IllegalArgumentException("Entity [" + entity + "] is not managed")
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
private[mnesia] class InMemoryStorage(schema: Map[Class[_], Table]) extends Storage(schema)

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
private[mnesia] class EvictableStorage(schema: Map[Class[_], Table], val timeout: Long) extends Storage(schema) {

  @serializable
  private sealed case class Evictable(val entity: AnyRef, val timeout: Long) {
    val timestamp: Long = timeNow
    def evict_? = timeNow - timestamp > timeout
  }

//   override def store(entity: AnyRef): PK = dbLock.withWriteLock {
//     val evictable = Evictable(entity, timeout)
//     val table = entity.getClass
//     ensureTableExists(table)

//     // update db
//     val pk = getPKFor(entity)
//     val pkIndex = PK.getPrimaryKeyColumnFor(table)
//     db(pkIndex) = db(pkIndex).upd(pk, evictable)

//     // update indices
//     schema(table).indices.foreach { item =>
//       val (column, field, indexFactory) = item
//       val indexMap = indices(column)
//       val index = createIndexFor(field, entity, indexFactory)
//       indexMap.get(index) match {
//         case Some(evictables) =>
//           evictables += getPKFor(entity) -> evictable
//         case None =>
//           addIndex(column.name, column.table, indexFactory)
//           indexMap.upd(index, Map[PK, AnyRef](getPKFor(entity) -> evictable))
//       }
//     }
//     pk
//   }

//   override def findByPK(pk: PK): Option[AnyRef] = dbLock.withReadLock {
//     ensureTableExists(pk.table)
    
//     db(PK.getPrimaryKeyColumnFor(pk.table)).get(pk) match {
//       case None => None
//       case Some(entity) => 
//         val evictable = entity.asInstanceOf[Evictable]
//         if (evictable.evict_?) {
//           remove(evictable.entity) // remove the evicted entity
//           None
//         } else Some(evictable.entity)
//     }
//   }

//   override def findByIndex(index: Index, column: Column): List[AnyRef] = indexLock.withReadLock {
//     ensureTableExists(column.table)
//     ensureIndexExists(column)
//     val evictables = 
//       indices(column).get(index).
//       getOrElse(throw new IllegalStateException("No such index <" + index + "> for column <" + column + ">")).
//       values.toList.asInstanceOf[List[Evictable]]
//     if (evictables.exists(_.evict_?)) { // if one evictable is found then evict the whole result set
//       evictables.foreach(remove(_))
//       List[AnyRef]()
//     }
//     else evictables.map(_.entity)
//   }

//   override def findAll(table: Class[_]): List[AnyRef] = dbLock.withReadLock {
//     ensureTableExists(table)
//     val evictables = db(PK.getPrimaryKeyColumnFor(table)).values.toList.asInstanceOf[List[Evictable]]
//     if (evictables.exists(_.evict_?)) { // if one evictable is found then evict the whole result set
//       evictables.foreach(evictable => remove(evictable.entity))
//       List[AnyRef]()
//     }
//     else evictables.map(_.entity)
//   }
}


