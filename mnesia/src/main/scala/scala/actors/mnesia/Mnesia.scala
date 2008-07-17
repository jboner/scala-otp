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
  case class Table(name: String, clazz: Class[_], columns: List[Column])
    case class Column(name: String, clazz: Class[_])
  
  case class PK(val id: Long) extends Ordered[PK] {
    override def compare(that: PK): Int = this.id.toInt - that.id.toInt
  }

  object PK {
    private val current = new AtomicLong(1)
    val id = "PK"    
    def next: PK = PK(current.getAndIncrement)
  }
  
  private object factory extends SupervisorFactory {
    override def getSupervisorConfig: SupervisorConfig = {
      SupervisorConfig(
        RestartStrategy(OneForOne, 5, 1000),
        Worker(
          mnesia,
          LifeCycle(Permanent, 100))
        :: Nil)
    }
  }

  private val mnesia = new GenericServerContainer("mnesia", () => new Mnesia)
  private val supervisor = factory.newSupervisor

  private val schema = Map[String, Table]()
  private val empty = TreapEmptyNode[PK, AnyRef]
  
  private val indexes = Map[String, Treap[PK, AnyRef]]()
  private val indexesLock = new ReadWriteLock

  
  def createTable(tableName: String, table: Class[_]): Mnesia.type = {
  //def createTable[T <: AnyRef](name: String, table: Class[T] forSome {type T}): Mnesia.type = {
    val columns: List[Column] = for (field <- table.getDeclaredFields.toList if field.getName != "$outer") yield Column(field.getName, field.getType)
    log.info("Creating table <{}> with columns <{}>", tableName, columns)        
    
    if (schema.contains(tableName)) throw new IllegalArgumentException("Table with name " + tableName + " already exists")
    schema += tableName -> Table(tableName, table, columns)
    
    indexesLock.withWriteLock {
      indexes += getIndex(tableName) -> new Treap[PK, AnyRef] 
    }
    this
  }
  
  def persist(tableName: String, record: AnyRef): PK = indexesLock.withWriteLock { 
    val index = getIndex(tableName)
    ensureIndexExists(index)
    val pk = PK.next
    indexes(index) = (indexes(index) + Pair(pk, record)).asInstanceOf[Treap[PK, AnyRef]]
    pk
  }
  
  def findBy(tableName: String, pk: PK): List[AnyRef] = indexesLock.withReadLock { 
    val index = getIndex(tableName)
    ensureIndexExists(index)
    indexes(index).elements.toList 
  }
 
  def findAll(tableName: String): List[AnyRef] = indexesLock.withReadLock { 
    val index = getIndex(tableName)
    ensureIndexExists(index)
    indexes(index).elements.toList 
  }
 
  def clear = indexesLock.withWriteLock { 
    log.info("Clearing database")
    indexes.clear    
    for (table <- schema.values) indexes += getIndex(table.name) -> new Treap[PK, AnyRef]
    indexes
 }
 
  def start = supervisor ! Start

  def stop = supervisor ! Stop

  private def ensureIndexExists(name: String) = if (!indexes.contains(name)) throw new IllegalArgumentException("Table [" + name + "] does not exist")

  private def getIndex(name: String): String = name + "::" + PK.id
}










private[mnesia] case class Bucket[V <: AnyRef](private var _value: V) {
  def value: V = synchronized { _value }
  def value_=(v: V) = synchronized { 
    if (v != null && _value != null) throw new IllegalStateException("Cannot overwrite an existing bucket value")
    _value = v
  }
}

private[mnesia] case class MnesiaTreapNode[K <% Ordered[K], V <: AnyRef](
  key: K,
  bucketValue: Bucket[V],
  bucketSelf:  Bucket[TreapNode[K, V]],
  bucketLeft:  Bucket[TreapNode[K, V]],
  bucketRight: Bucket[TreapNode[K, V]])
  extends TreapFullNode[K, V] {

  def left(t: T)  = bucketLeft.value
  def right(t: T) = bucketRight.value
  def value(t: T) = bucketValue.value
}

