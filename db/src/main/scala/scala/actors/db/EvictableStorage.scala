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
private[db] class EvictableStorage(schema: Map[Class[_], Table], val timeout: Long) extends Storage(schema) {

  @serializable
  private sealed case class Evictable(val entity: AnyRef, val timeout: Long) {
    val timestamp: Long = timeNow
    def evict_? = timeNow - timestamp > timeout
  }

  override def store(entity: AnyRef): Index = {
    val evictable = Evictable(entity, timeout)

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
    db(pkColumn) = db(pkColumn).upd(pkIndex, evictable)

    // update indices
    schema(tableClass).indices.foreach { item =>
      val (column, field, indexFactory) = item
      val indexTreap = indices(column)
      val index = getIndexFor(entity, column.name)
      indexTreap.get(index) match {
        case Some(evictables) =>
          evictables += getPKIndexFor(entity) -> evictable
       case None =>
          addIndex(column.name, column.table, indexFactory)
          indices(column) = indexTreap.upd(index, Map[Index, AnyRef](getPKIndexFor(entity) -> evictable))
      }
    }
    pkIndex
  }

  override def findByPK(pkIndex: Index, table: Class[_]): Option[AnyRef] = { 
    ensureTableExists(table)
    val pkColumn = getPKColumnFor(table)
    db(pkColumn).get(pkIndex) match {
      case None => None
      case Some(entity) => 
        val evictable = entity.asInstanceOf[Evictable]
        if (evictable.evict_?) {
          remove(evictable.entity) // remove the evicted entity
          None
        } else Some(evictable.entity)
    }
  }

  override def findByIndex(index: Index, columnName: String, table: Class[_]): List[AnyRef] = {
    ensureTableExists(table)
    val column = getColumnFor(columnName, table)
    ensureIndexExists(column)
    val evictables = 
      indices(column).get(index).
      getOrElse(throw new IllegalStateException("No such index <" + columnName + "> for table <" + table.getName + ">")).
      values.toList.asInstanceOf[List[Evictable]]
    if (evictables.exists(_.evict_?)) { // if one evictable is found then evict the whole result set
      evictables.foreach(remove(_))
      List[AnyRef]()
    }
    else evictables.map(_.entity)
  }

  override def findAll(table: Class[_]): List[AnyRef] = {
    ensureTableExists(table)
    val evictables = db(getPKColumnFor(table)).values.toList.asInstanceOf[List[Evictable]]
    if (evictables.exists(_.evict_?)) { // if one evictable is found then evict the whole result set
      evictables.foreach(evictable => remove(evictable.entity))
      List[AnyRef]()
    }
    else evictables.map(_.entity)
  }
}


