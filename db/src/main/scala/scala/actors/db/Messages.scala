/**
 * Copyright (C) 2007-2008 Scala OTP Team
 */

package scala.actors.db

import scala.collection.mutable.Map

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
  val schema = new CovariantMap[Class[_], Table]()
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
/** Reply message: Status(..) */
@serializable
case class CreateTableWithGeneratedPK(table: Class[_], pkName: String) extends DBMessage
/** Reply message: Status(..) */
@serializable
case class CreateTableWithCustomPK(table: Class[_], pkName: String, pkIndexFactory: (Any) => Index) extends DBMessage 

/** Reply message: Status(..) */
@serializable
private[db] case class FindTreap(columnName: String, table: Class[_]) extends DBMessage
/** Reply message: Status(..) */
@serializable
case class AddIndex(name: String, table: Class[_], indexFactory: (Any) => Index) extends DBMessage
/** Reply message: Status(..) */
@serializable
case class Clear extends DBMessage

/** Reply message: Status(..) */
@serializable
case class RemoveByEntity(entity: AnyRef) extends DBMessage
/** Reply message: Status(..) */
@serializable
case class RemoveByPK(pkName: Index, table: Class[_]) extends DBMessage
/** Reply message: Status(..) */
@serializable
case class RemoveByIndex(pkName: Index, columnName: String, table: Class[_]) extends DBMessage

/** Reply message: PrimaryKey(..) */
@serializable
case class Store(entity: AnyRef) extends DBMessage

/** Reply message: Entity(..) */
@serializable
case class FindByPK(index: Index, table: Class[_]) extends DBMessage
/** Reply message: Entities(..) */
@serializable
case class FindByIndex(index: Index, columnName: String, table: Class[_]) extends DBMessage
/** Reply message: Entities(..) */
@serializable
case class FindAll(table: Class[_]) extends DBMessage

/** Reply message: PrimaryKey(..) */
@serializable
case class FirstPK(table: Class[_]) extends DBMessage
/** Reply message: PrimaryKey(..) */
@serializable
case class LastPK(table: Class[_]) extends DBMessage
/** Reply message: PrimaryKey(..) */
@serializable
case class FirstIndex(columnName: String, table: Class[_]) extends DBMessage
/** Reply message: PrimaryKey(..) */
@serializable
case class LastIndex(columnName: String, table: Class[_]) extends DBMessage
/** Reply message: Entities(..) */
@serializable
case class RangePK(from: Index, until: Index, table: Class[_]) extends DBMessage
/** Reply message: Entities(..) */
@serializable
case class RangeIndex(from: Index, until: Index, columnName: String, table: Class[_]) extends DBMessage
/** Reply message: Entities(..) */
@serializable
case class RangeFromPK(from: Index, table: Class[_]) extends DBMessage
/** Reply message: Entities(..) */
@serializable
case class RangeFromIndex(from: Index, columnName: String, table: Class[_]) extends DBMessage
/** Reply message: Entities(..) */
@serializable
case class RangeUntilPK(until: Index, table: Class[_]) extends DBMessage
/** Reply message: Entities(..) */
@serializable
case class RangeUntilIndex(until: Index, columnName: String, table: Class[_]) extends DBMessage

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
private[db] case class TreapForTable(treap: AnyRef) extends DBMessage

//-----------------------
// --- STATUS MESSAGES ---
//-----------------------
@serializable
abstract case class Status extends DBMessage
@serializable
case object Success extends Status
@serializable
case class Failure(message: String, cause: Option[Throwable]) extends Status


