package akkax.actor.scheduling
package mapdb

import akka.actor.{Cancellable, ActorRef}
import org.mapdb.{DB, DBMaker}
import java.io.File
import java.util.concurrent.ConcurrentNavigableMap
import java.util.concurrent.atomic.AtomicBoolean
import collection.JavaConverters._

class MapDBMemoryScheduledMessageQueue(file: File, password: Option[String] = None) extends ScheduledMessageQueue { self =>
  val db: DB = {
    val db = DBMaker.newFileDB(file).closeOnJvmShutdown()
    password.foreach(db.encryptionEnable)
    db.make()
  }
  val map: ConcurrentNavigableMap[Long, Array[ScheduledMessage]] = db.getTreeMap("akkax-scheduling-map")

  private [this] val sentinelKey = Long.MaxValue
  private [this] def fetchSentinel: Option[Long] = Option(map.get(sentinelKey)).map(_.apply(0).expression.toLong)
  private [this] def storeSentinel(value: Long) = map.replace(sentinelKey, Array(ScheduledMessage(null, null, null, value.toString)))

  def enqueue(time: Long, scheduledMessage: ScheduledMessage) = {
    scheduledMessage.nextOccurrence.map { time =>
      var array: Array[ScheduledMessage] = null
      var replaced: Boolean = false
      do {
        array = map.putIfAbsent(time, Array(scheduledMessage))
        if (array != null) {
          val buffer = array.toBuffer
          buffer.append(scheduledMessage)
          replaced = map.replace(time, array, buffer.toArray)
        }
      } while (array != null && !replaced)
    }
    None
  }


  override def enqueued(time: Long, scheduledMessage: ScheduledMessage) = db.commit()

  def peek(timestamp: Long): Iterable[ScheduledMessage] = {
    val subMap = map.subMap(fetchSentinel.getOrElse(0), true, timestamp, false)
    subMap.values().asScala.flatten.toIterable
  }

  def dequeue(timestamp: Long): Iterable[ScheduledMessage] = {
    val messages = peek(timestamp)
    messages.map { sm => sm.nextOccurrence -> sm }.collect { case (Some(t), sm) => t -> sm }.foreach {
      case (time, sm) => enqueue(time, sm)
    }
    storeSentinel(timestamp)
    messages
  }


  override def dequeued(time: Long) = db.commit()

  def cancel(sm: ScheduledMessage): Unit = {
    sm.nextOccurrence.map { time =>
      var array: Array[ScheduledMessage] = null
      var removed: Boolean = false
      do {
        array = map.get(time)
        if (array != null && array.length == 1 && array(0) == sm) removed = map.remove(time, array)
        else if (array != null) removed = map.replace(time, array, array.filterNot(_ == sm))
      } while (array != null && !removed)
    }
  }

  override def cancelled(scheduledMessage: ScheduledMessage) = db.commit()
}