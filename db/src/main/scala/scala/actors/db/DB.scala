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
object DB extends DB

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class DB extends Logging {

  val SERVER_NAME = "GPS_SERVER_CACHE"
  val SHUTDOWN_TIME = 1000
  val RESTART_WITHIN_TIME_RANGE = 1000
  val MAX_NUMBER_OF_RESTART_BEFORE_GIVING_UP = 5
  val INVOCATION_TIME_OUT = 10000

  private val isInitialized = new AtomicBoolean(false)
  private val isStarted = new AtomicBoolean(false)

  private val db = new GenericServerContainer(SERVER_NAME, () => new DBServer)
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

  def init(config: Config): DB =  {
    if (!isInitialized.getAndSet(true)) db ! Init(config)
    this
  }
  
  def start: DB = {
    if (!isInitialized.get) throw new IllegalStateException("DB is not initialized, make sure that the <DB.init(config)> is invoked prior to <DB.start>")
    if (!isStarted.getAndSet(true)) supervisor ! Start
    this
  }
  
  def stop = {
    if (!isStarted.get) throw new IllegalStateException("DB is not started")
    if (isStarted.getAndSet(false)) supervisor ! Stop
  }

  def clear = handleStatus(db !!! (Clear(), throw new DBException("Timed out while trying to clear the database"), INVOCATION_TIME_OUT))

  def newQueryBuilderFor[K <% Ordered[K], V <: AnyRef](columnName: String, table: Class[_]) = { 
    val treap = getTreapFor[K, V](columnName, table)
    new QueryBuilder[K, V](treap, this)
  }

  // FIXME: implement correctly
  def execute[K <% Ordered[K], V <: AnyRef, T <: Treap[K, V]](body: => T): T = { 
    body
  }

  def createTable(table: Class[_], pkName: String): DB = {
    handleStatus(db !!! (CreateTableWithGeneratedPK(table, pkName), throw new DBException("Timed out while trying to create table with auto generated primary key"), INVOCATION_TIME_OUT))
    this
  }

  def createTable(table: Class[_], pkName: String, pkIndexFactory: (Any) => Index): DB = {
    handleStatus(db !!! (CreateTableWithCustomPK(table, pkName, pkIndexFactory), throw new DBException("Timed out while trying to create table with custom primary key"), INVOCATION_TIME_OUT))
    this
  }

  def addIndex(columnName: String, table: Class[_], indexFactory: (Any) => Index): DB = {
    handleStatus(db !!! (AddIndex(columnName, table, indexFactory), throw new DBException("Timed out while trying to add index"), INVOCATION_TIME_OUT))
    this
  }

  def store(entity: AnyRef): Index = {
    val result: DBMessage = db !!! (Store(entity), throw new DBException("Timed out while trying to store entity"), INVOCATION_TIME_OUT)
    result match {
      case PrimaryKey(pkIndex) => pkIndex
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
  }

  def remove(entity: AnyRef) = 
    handleStatus(db !!! (RemoveByEntity(entity), throw new DBException("Timed out while trying to remove entity by reference"), INVOCATION_TIME_OUT))

  def removeByPK(pkIndex: Index, table: Class[_]) = 
    handleStatus(db !!! (RemoveByPK(pkIndex, table), throw new DBException("Timed out while trying to remove entity by primary key"), INVOCATION_TIME_OUT))

  def removeByIndex(index: Index, columnName: String, table: Class[_]) = 
    handleStatus(db !!! (RemoveByIndex(index, columnName, table), throw new DBException("Timed out while trying to remove entity by index"), INVOCATION_TIME_OUT))

  def findByPK[T <: AnyRef](index: Index, table: Class[_]): Option[T] = {
    val result: DBMessage = db !!! (FindByPK(index, table), throw new DBException("Timed out while trying to find entity by primary key"), INVOCATION_TIME_OUT)
    result match {
      case Entity(entity) => entity.asInstanceOf[Option[T]]
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
 }

  def findByIndex[T <: AnyRef](index: Index, columnName: String, table: Class[_]): List[T] = {
    val result: DBMessage = db !!! (FindByIndex(index, columnName, table), throw new DBException("Timed out while trying to find entity by index"), INVOCATION_TIME_OUT)
    result match {
      case Entities(entities) => entities.asInstanceOf[List[T]]
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
 }

  def findAll[T <: AnyRef](table: Class[_]): List[T] = {
    val result: DBMessage = db !!! (FindAll(table), throw new DBException("Timed out while trying to find all entities"), INVOCATION_TIME_OUT)
    result match {
      case Entities(entities) => entities.asInstanceOf[List[T]]
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
  }

  /**
   * Returns the last key of the table.
   */
  def firstPK(table: Class[_]): Index = {
    val result: DBMessage = db !!! (FirstPK(table), throw new DBException("Timed out while trying to find last primary key"), INVOCATION_TIME_OUT)
    result match {
      case PrimaryKey(pk) => pk
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
  }

  /**
   * Returns the last key of the table.
   */
  def lastPK(table: Class[_]): Index = {
    val result: DBMessage = db !!! (LastPK(table), throw new DBException("Timed out while trying to find last primary key"), INVOCATION_TIME_OUT)
    result match {
      case PrimaryKey(pk) => pk
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
  }

  /**
   * Creates a ranged projection of this collection with both a lower-bound and an upper-bound.
   */
  def rangePK[T <: AnyRef](from: Index, until: Index, table: Class[_]): List[T] = {
    val result: DBMessage = db !!! (RangePK(from, until, table), throw new DBException("Timed out while trying to find a range of entities"), INVOCATION_TIME_OUT)
    result match {
      case Entities(entities) => entities.asInstanceOf[List[T]]
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
  }

  /**
   * Creates a ranged projection of this collection with no upper-bound.
   */
  def rangeFromPK[T <: AnyRef](from: Index, table: Class[_]) : List[T] = {
    val result: DBMessage = db !!! (RangeFromPK(from, table), throw new DBException("Timed out while trying to find a range of entities"), INVOCATION_TIME_OUT)
    result match {
      case Entities(entities) => entities.asInstanceOf[List[T]]
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
  }

  /**
   * Creates a ranged projection of this collection with no lower-bound.
   */
  def rangeUntilPK[T <: AnyRef](until: Index, table: Class[_]): List[T] = {
    val result: DBMessage = db !!! (RangeUntilPK(until, table), throw new DBException("Timed out while trying to find a range of entities"), INVOCATION_TIME_OUT)
    result match {
      case Entities(entities) => entities.asInstanceOf[List[T]]
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
  }

  /**
   * Returns the first index of the table.
   */
  def firstIndex(columnName: String, table: Class[_]): Index = {
    val result: DBMessage = db !!! (FirstIndex(columnName, table), throw new DBException("Timed out while trying to find last index"), INVOCATION_TIME_OUT)
    result match {
      case PrimaryKey(pk) => pk
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
  }

  /**
   * Returns the last index of the table.
   */
  def lastIndex(columnName: String, table: Class[_]): Index = {
    val result: DBMessage = db !!! (LastIndex(columnName, table), throw new DBException("Timed out while trying to find last index"), INVOCATION_TIME_OUT)
    result match {
      case PrimaryKey(pk) => pk
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
  }

  /**
   * Creates a ranged projection of this collection with both a lower-bound and an upper-bound.
   */
  def rangeIndex[T <: AnyRef](from: Index, until: Index, columnName: String, table: Class[_]): List[T] = {
    val result: DBMessage = db !!! (RangeIndex(from, until, columnName, table), throw new DBException("Timed out while trying to find a range of entities"), INVOCATION_TIME_OUT)
    result match {
      case Entities(entities) => entities.asInstanceOf[List[T]]
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
  }

  /**
   * Creates a ranged projection of this collection with no upper-bound.
   */
  def rangeFromIndex[T <: AnyRef](from: Index, columnName: String, table: Class[_]) : List[T] = {
    val result: DBMessage = db !!! (RangeFromIndex(from, columnName, table), throw new DBException("Timed out while trying to find a range of entities"), INVOCATION_TIME_OUT)
    result match {
      case Entities(entities) => entities.asInstanceOf[List[T]]
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
  }

  /**
   * Creates a ranged projection of this collection with no lower-bound.
   */
  def rangeUntilIndex[T <: AnyRef](until: Index, columnName: String, table: Class[_]): List[T] = {
    val result: DBMessage = db !!! (RangeUntilIndex(until, columnName, table), throw new DBException("Timed out while trying to find a range of entities"), INVOCATION_TIME_OUT)
    result match {
      case Entities(entities) => entities.asInstanceOf[List[T]]
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }
  }

  private def getTreapFor[K <% Ordered[K], V <: AnyRef](columnName: String, table: Class[_]) = { 
    val result: DBMessage = db !!! (FindTreap(columnName, table), throw new DBException("Timed out while trying to find treap"), INVOCATION_TIME_OUT)
    result match {
      case TreapForTable(treap) => treap.asInstanceOf[Treap[K, V]]
      case failure: Failure => handleFailure(failure)
      case msg => throw new IllegalStateException("Received unexpected message: " + msg)
    }    
  }

  private def handleStatus[T](status: Status): Unit = handleStatus(status, {})

  private def handleStatus[T](status: Status, defaultReturnValue: => T): T = status match {
    case failure: Failure => handleFailure(failure)
    case Success => defaultReturnValue
    case _ => defaultReturnValue
  }

  private def handleFailure(failure: Failure): Nothing = failure match {
    case Failure(_, Some(cause)) => throw cause
    case Failure(message, None) => throw new DBException(message)
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
private[db] class DBServer extends GenericServer with Logging {

  private var storage: Storage = _

  def createStorage(strategy: StorageStrategy, schema: Map[Class[_], Table]) = strategy match {
    case InMemoryStorageStrategy => new InMemoryStorage(schema)
    case EvictableStorageStrategy(timeout: Long) => new EvictableStorage(schema, timeout)
    case DiskBasedStorageStrategy => throw new UnsupportedOperationException("DiskBasedStorageStrategy is yet to be implemented")
    case ReplicatedAtomicStorageStrategy => throw new UnsupportedOperationException("ReplicatedAtomicStorageStrategy is yet to be implemented")
    case ReplicatedEventuallyConsistentStorageStrategy => throw new UnsupportedOperationException("ReplicatedEventuallyConsistentStorageStrategy is yet to be implemented")
  }

  override def init(config: AnyRef) = {
    val cfg = config.asInstanceOf[Config]
    storage = createStorage(cfg.storageStrategy, cfg.schema)
    log.info("Database initialized: schema <{}> - storage strategy <{}>", cfg.schema, cfg.storageStrategy)
  }

  /*override def reinit(config: AnyRef) = {
    val cfg = config.asInstanceOf[Config]
    storage = createStorage(cfg.storageStrategy, cfg.schema)
    log.info("Database reinitialized after server crash: schema <{}> - storage strategy <{}>", cfg.schema, cfg.storageStrategy)

    // TODO: reinit DB
  }*/

  override def body: PartialFunction[Any, Unit] = {
    
    case CreateTableWithGeneratedPK(table, pkName) =>
      log.info("Creating table <{}>", table)
      try {
        storage.createTable(table, pkName)
        reply(Success)
      } catch {
        case e => 
          log.error("Could not create table due to: {}", e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case CreateTableWithCustomPK(table, pkName, pkIndexFactory) =>
      log.info("Creating table <{}>", table)
      try {
        storage.createTable(table, pkName, pkIndexFactory)
        reply(Success)
      } catch {
        case e => 
          log.error("Could not create table due to: {}", e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case AddIndex(columnName, table, indexFactory) =>
      log.info("Adding index <{}@{}>", columnName, table.getName)
      try {
        storage.addIndex(columnName, table, indexFactory)
        reply(Success)
      } catch {
        case e => 
          log.error("Could not create index due to: {}", e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case FindTreap(columnName, table) =>
      try {
        reply(TreapForTable(storage.findTreapFor(columnName, table)))
      } catch {
        case e => 
          log.error("Could not create index due to: {}", e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case Store(entity) =>
      log.debug("Storing entity <{}>", entity)
      try {
        reply(PrimaryKey(storage.store(entity)))
      } catch {
        case e => 
          log.error("Could not store entity <{}> due to: {}", entity, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case RemoveByEntity(entity) =>
      log.debug("Removing entity <{}>", entity)
      try {
        storage.remove(entity)
        reply(Success)
      } catch {
        case e => 
          log.error("Could not remove entity <{}> due to: {}", entity, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case RemoveByPK(pkIndex, table) =>
      log.debug("Removing entity with primary key <{}> and table <{}>", pkIndex, table)
      try {
        storage.removeByPK(pkIndex, table)
        reply(Success)
      } catch {
        case e => 
          log.error("Could not remove entity with primary key <{}> due to: {}", pkIndex, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case RemoveByIndex(index, columnName, table) =>
      log.debug("Removing entity with index <{}> and table <{}>", index, table)
      try {
        storage.removeByIndex(index, columnName, table)
        reply(Success)
      } catch {
        case e => 
          log.error("Could not remove entity with index <{}> due to: {}", index, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case FindByPK(index, table) =>
      log.debug("Finding entity by primary key <{}> and table <{}>", index, table)
      try {
        reply(Entity(storage.findByPK(index, table)))
      } catch {
        case e => 
          log.error("Could not find entity by primary key <{}> due to: {}", index, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case FindByIndex(index, columnName, table) =>
      log.debug("Finding entity by index <{}> and table <{}>", columnName, table)
      try {
        reply(Entities(storage.findByIndex(index, columnName, table)))
      } catch {
        case e => 
          log.error("Could not find entity by index <{}> due to: {}", index, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case FindAll(table) =>
      log.debug("Finding all entities in table <{}>", table.getName)
      try {
        reply(Entities(storage.findAll(table)))
      } catch {
        case e => 
          log.error("Could not find all entities in table <{}> due to: {}", table.getName, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case FirstPK(table) =>
      log.debug("Finding first primary key for <{}>", table)
      try {
        reply(PrimaryKey(storage.firstPK(table)))
      } catch {
        case e => 
          log.error("Could not find primary key for <{}> due to: {}", table, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case LastPK(table) =>
      log.debug("Finding last primary key for <{}>", table)
      try {
        reply(PrimaryKey(storage.lastPK(table)))
      } catch {
        case e => 
          log.error("Could not find primary key for <{}> due to: {}", table, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case FirstIndex(columnName, table) =>
      log.debug("Finding first index for <{}>:<{}>", columnName, table)
      try {
        reply(PrimaryKey(storage.firstIndex(columnName, table)))
      } catch {
        case e => 
          log.error("Could not find primary key for <{}> due to: {}", table, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case LastIndex(columnName, table) =>
      log.debug("Finding last index for <{}>:<{}>", columnName, table)
      try {
        reply(PrimaryKey(storage.lastIndex(columnName, table)))
      } catch {
        case e => 
          log.error("Could not find primary key for <{}> due to: {}", table, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case RangePK(from, until, table) =>
      log.debug("Finding all entities in primary key range <{}> to <{}> in table <" + table.getName + ">", from, until)
      try {
        reply(Entities(storage.rangePK(from, until, table)))
      } catch {
        case e => 
          log.error("Could not not find entities by primary key range in table <{}> due to: {}", table.getName, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case RangeIndex(from, until, columnName, table) =>
      log.debug("Finding all entities in index range <{}> to <{}> in table <" + table.getName + ">", from, until)
      try {
        reply(Entities(storage.rangeIndex(from, until, columnName, table)))
      } catch {
        case e => 
          log.error("Could not not find entities by index range in table <{}> due to: {}", table.getName, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case RangeFromPK(from, table) =>
      log.debug("Finding all entities from primary key range <{}> in table <" + table.getName + ">", from)
      try {
        reply(Entities(storage.rangeFromPK(from, table)))
      } catch {
        case e => 
          log.error("Could not not find entities from primary key range in table <{}> due to: {}", table.getName, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case RangeFromIndex(from, columnName, table) =>
      log.debug("Finding all entities from index range <{}> in table <" + table.getName + ">", from)
      try {
        reply(Entities(storage.rangeFromIndex(from, columnName, table)))
      } catch {
        case e => 
          log.error("Could not not find entities from index range in table <{}> due to: {}", table.getName, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case RangeUntilPK(until, table) =>
      log.debug("Finding all entities until primary key range <{}> in table <" + table.getName + ">", until)
      try {
        reply(Entities(storage.rangeUntilPK(until, table)))
      } catch {
        case e => 
          log.error("Could not not find entities until primary key range in table <{}> due to: {}", table.getName, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case RangeUntilIndex(until, columnName, table) =>
      log.debug("Finding all entities until index range <{}> in table <" + table.getName + ">", until)
      try {
        reply(Entities(storage.rangeUntilIndex(until, columnName, table)))
      } catch {
        case e => 
          log.error("Could not not find entities until index range in table <{}> due to: {}", table.getName, e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }

    case Clear() =>
      log.info("Clearing and reinitializes the database")
      try {
        storage.clear
        log.info("Database cleared successfully")
        reply(Success)
      } catch { 
        case e =>
          log.error("Database could not be cleared due to: {}", e.getMessage)
          reply(Failure(e.getMessage, Some(e)))
      }
  }
}

