package akkax.scheduling
package mapdb

import collection.JavaConverters._
import akka.actor.{ActorPath, Cancellable, ActorRef}
import java.io.File
import java.util.concurrent.ConcurrentNavigableMap
import java.util.concurrent.atomic.AtomicBoolean
import org.mapdb.{DB, DBMaker}

class MapDBSchedulingQueue(file: File, password: Option[String] = None) extends SchedulingQueue { self =>
  val db: DB = {
    val db = DBMaker.newFileDB(file).closeOnJvmShutdown()
    password.foreach(db.encryptionEnable)
    db.make()
  }
  val map: ConcurrentNavigableMap[Long, Array[ScheduledMessage]] = db.getTreeMap("akkax-scheduling-map")
  println(map.asScala.mkString("!!! Loaded persistent messages:", "\n\t", "\n"))

  val committing = new AtomicBoolean(false)

  private [this] val sentinelKey = Long.MaxValue
  private [this] def fetchSentinel: Option[Long] = Option(map.putIfAbsent(sentinelKey, Array(ScheduledMessage(None, null: ActorPath, null, "0")))).map(_.apply(0).expression.toLong)
  private [this] def storeSentinel(value: Long) = {
    map.replace(sentinelKey, Array(ScheduledMessage(None: Option[ActorPath], null, null, value.toString)))
  }

  private [this] def commit() {
    if (committing.compareAndSet(false, true)) {
      db.commit()
      committing.compareAndSet(true, false)
    }
  }

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


  override def enqueued(time: Long, scheduledMessage: ScheduledMessage) = commit()

  def peek(timestamp: Long): Iterable[ScheduledMessage] = {
    val subMap = map.subMap(fetchSentinel.getOrElse(0), true, timestamp, false)
    subMap.values().asScala.flatten.toIterable
  }

  def dequeue(timestamp: Long): Iterable[ScheduledMessage] = {
    val messages = peek(timestamp)
    if (messages.nonEmpty) {
      println("messages(" + timestamp + "): " + messages.map(_.message).mkString(", "))
      messages.map { sm => sm.nextOccurrence -> sm }.collect { case (Some(t), sm) => t -> sm }.foreach {
        case (time, sm) => enqueue(time, sm)
      }
      storeSentinel(timestamp)
    }
    messages
  }

  override def dequeued(time: Long) = commit()

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

  override def cancelled(scheduledMessage: ScheduledMessage) = commit()
}