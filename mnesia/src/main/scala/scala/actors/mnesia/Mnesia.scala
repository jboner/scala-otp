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
case class Table(clazz: Class[_], columns: List[Column]) {
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
abstract case class Index extends Ordered[Index] {
  type T
  val value: T
  def compare(that: Index): Int = this.hashCode - that.hashCode
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
case class StringIndex(override val value: String) extends Index {
  type T = String
  def compare(that: StringIndex): Int = this.hashCode - that.hashCode
  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, value)
    result
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object StringIndex {
  def newInstance: String => Index = (value: String) => StringIndex(value.asInstanceOf[String])
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
private[mnesia] class Mnesia extends GenericServer with Logging {
  override def body: PartialFunction[Any, Unit] = {
    case Start => log.info("Starting up Mnesia...")
    case Stop => log.info("Shutting down Mnesia...")
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object Mnesia extends Logging {
  val DEFAULT_SCHEMA = "default"
  val MNESIA_SERVER_NAME = "mnesia"

  /**
   * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
   */
  object PK {
    val name = "PK"
    private val keys = Map[Class[T] forSome {type T}, AtomicLong]()

    def init(table: Class[_]) = {
      val currrentIndex =
        if (db.contains(PK.indexFor(table))) {
          val treap =  db(PK.indexFor(table))
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

  /**
   * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
   */
  case class PK(value: Long, table: Class[_]) extends Ordered[PK] {
    val index = Column(PK.name, table)
    def compare(that: PK): Int = this.value.toInt - that.value.toInt
  }

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

  private val TREAP_CLASS = Class.forName("scala.actors.mnesia.Treap")
  private val METHOD_TREAP_UPD = TREAP_CLASS.getDeclaredMethod("upd", Array(classOf[AnyRef], classOf[AnyRef]))

  private var currentSchema = DEFAULT_SCHEMA
  private val mnesia = new GenericServerContainer(MNESIA_SERVER_NAME, () => new Mnesia)
  private val supervisor = supervisorFactory.newSupervisor

  // FIXME: update to ConcurrentHashMaps
  private val schema =      Map[Class[_]  , Table]()
  private val db =          Map[Column, Treap[PK, AnyRef]]()
  private val indexes =     Map[Column, Treap[Index, AnyRef]]()
  private val identityMap = Map[AnyRef, PK]()
  private val dbLock =      new ReadWriteLock
  private val indexLock =   new ReadWriteLock

  def createTable(table: Class[_]): Mnesia.type = {
    val columns: List[Column] = for (field <- table.getDeclaredFields.toList if field.getName != "$outer") yield Column(field.getName, field.getType)
    log.info("Creating table <{}> with columns <{}>", table.getName, columns)

    if (schema.contains(table)) throw new IllegalArgumentException("Table <" + table.getName + "> already exists")
    schema += table -> Table(table, columns)

    dbLock.withWriteLock {
      PK.init(table)
      db += PK.indexFor(table) -> new Treap[PK, AnyRef]
    }
    this
  }

  def addIndex[I](name: String, table: Class[_], indexFactory: (I) => Index) = dbLock.withWriteLock { indexLock.withWriteLock {
    val field =
      try { table.getDeclaredField(name) }
      catch { case e => throw new IllegalArgumentException("Could not create index <" + name + "> for table <" + table.getName + "> due to: " + e.toString) }
    field.setAccessible(true)
    val currentDB = db(PK.indexFor(table))
    val emptyTreap = new Treap[Index, AnyRef]()
    val fullTreap = currentDB.elements.foldLeft(emptyTreap) { (treap, entry) => {
      val entity = entry._2
      val index = field.get(entity).asInstanceOf[I]
      treap.upd(indexFactory(index), entity)
    }}
    indexes += Column(name, table) -> fullTreap
    log.info("Index <{}> for table <{}> has been compiled.", name, table.getName)
  }}

  def store(entity: AnyRef): PK = dbLock.withWriteLock {
    val table = entity.getClass
    ensureTableExists(table)
    val pk = if (identityMap.contains(entity)) identityMap(entity)
             else PK.next(table)
    db(pk.index) = db(pk.index).upd(pk, entity)
    identityMap(entity) = pk
    pk
  }

  def remove(entity: AnyRef) = dbLock.withWriteLock { indexLock.withWriteLock {
    val table = entity.getClass
    ensureTableExists(table)
    ensureEntityExists(entity)
    val pk = identityMap(entity)
    val newTreap = db(pk.index).del(pk)
    db(pk.index) = newTreap
    identityMap - entity

    // FIXME: remove entity from all (0..N) indexes it is stored in
  }}

  def remove(pk: PK) = dbLock.withWriteLock { indexLock.withWriteLock {
    ensureTableExists(pk.table)
    val entity = db(pk.index).get(pk)
    val newTreap = db(pk.index).del(pk)
    db(pk.index) = newTreap
    identityMap - entity

    // FIXME: remove entity from all (0..N) indexes it is stored in
  }}

  def findByPK(pk: PK): Option[AnyRef] = dbLock.withReadLock {
    ensureTableExists(pk.table)
    db(pk.index).get(pk)
  }

  // FIXME: should be able to look up column from index and not have to specific both
  def findByIndex(index: Index, column: Column): Option[AnyRef] = indexLock.withReadLock {
    ensureTableExists(column.table)
    ensureIndexExists(column)
    indexes(column).get(index)
  }

  def findAll(table: Class[_]): List[AnyRef] = dbLock.withReadLock {
    ensureTableExists(table)
    db(PK.indexFor(table)).elements.toList
  }

  def clear = dbLock.withWriteLock { indexLock.withWriteLock {
    log.info("Clearing schema {}.", currentSchema)
    db.clear
    identityMap.clear
    indexes.clear
    for (table <- schema.values) db += PK.indexFor(table.clazz) -> new Treap[PK, AnyRef]
  }}

  def changeSchema(schema: String) = {
    currentSchema = schema
    // FIXME: implement support for multiple schemas
  }

  def start = supervisor ! Start

  def stop = supervisor ! Stop

  private def ensureTableExists(table: Class[_]) = dbLock.withReadLock {
    if (!db.contains(Column(PK.name, table))) throw new IllegalArgumentException("Table [" + table.getName + "] does not exist")
  }

  private def ensureIndexExists(index: Column) = indexLock.withReadLock {
    if (!indexes.contains(index)) throw new IllegalArgumentException("Index [" + index + "] does not exist")
  }

  private def ensureEntityExists(entity: AnyRef) = dbLock.withReadLock {
    if (!identityMap.contains(entity)) throw new IllegalArgumentException("Entity [" + entity + "] is not managed")
  }
}
