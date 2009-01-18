package scala.actors.db

class CovariantMap[A, B] {
  private[this] val map = new scala.collection.mutable.HashMap[Any, B]
  def apply[AA <: A](key: AA): B = map(key)
  def update[AA <: A](key: AA, value: B) = map(key) = value
  def contains[AA <: A](key: AA) = map.contains(key)
  def values = map.values
}
