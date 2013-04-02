package akkax.actor.scheduling
package memory

import akka.actor.{Cancellable, ActorRef}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.{SortedMap, Iterable}

class MemoryScheduledMessageQueue extends ScheduledMessageQueue { self =>
  var queue = SortedMap[Long, ScheduledMessage]()

  def enqueue(time: Long, sm: ScheduledMessage) = {
    queue += time -> sm
    None
  }

  def peek(timestamp: Long): Iterable[ScheduledMessage] = queue.until(timestamp).values

  def dequeue(timestamp: Long): Iterable[ScheduledMessage] = {
    val messages = peek(timestamp)
    queue = queue.from(timestamp)
    queue ++= messages.map { sm => sm.nextOccurrence -> sm }.collect { case (Some(t), sm) => t -> sm }
    messages
  }


  def cancel(sm: ScheduledMessage) {
    queue = queue.filterNot{ t =>
      //println("cancel: " + t)
      t._2 == sm
    }
  }
}

