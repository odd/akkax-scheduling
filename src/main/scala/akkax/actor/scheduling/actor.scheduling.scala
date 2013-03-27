package akkax.actor.scheduling

import collection._
import scala.util.matching.Regex
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}
import javax.naming.OperationNotSupportedException
import java.util.{TimeZone, Calendar, GregorianCalendar}
import akka.actor._
import scala.Some
import akkax.actor.scheduling.Cancel
import akkax.actor.scheduling.Schedule

package object scheduling {
  implicit def enrichActorRef(actorRef: ActorRef): RichActorRef = new RichActorRef(actorRef)
}
class RichActorRef(actorRef: ActorRef) {
  def !@(messageWithExpression: Pair[Any, String])(implicit sender: ActorRef = null): Cancellable = {
    InMemoryScheduledMessageQueue.enqueue(sender = Option(sender), receiver = actorRef, message = messageWithExpression._1, expression = messageWithExpression._2)
  }

  def tellAt(message: Any, expression: String)(): Cancellable = this.!@((message, expression))(null: ActorRef)

  def tellAt(message: Any, expression: String, sender: ActorRef): Cancellable = this.!@((message, expression))(sender)
}

case class ScheduledMessage(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String) {
  def nextOccurrence: Option[Long] = expression match {
    case ScheduledMessage.Nanos(millis) =>
      val time = millis.toLong
      if (time >= System.currentTimeMillis()) Some(time)
      else None
    case ScheduledMessage.Timestamp(year, month, day, hour, minute, second, millis, timeZone) =>
      def int(str: String) = if (str == null || str == "") 0 else str.toInt
      val calendar = new GregorianCalendar(int(year), int(month) - 1, int(day), int(hour), int(minute), int(second))
      if (timeZone != null && timeZone != "") calendar.setTimeZone(TimeZone.getTimeZone("GMT" + timeZone))
      calendar.set(Calendar.MILLISECOND, int(millis))
      val time = calendar.getTimeInMillis
      if (time >= System.currentTimeMillis()) Some(time) else None
    case _ =>
      sys.error("Unknown schedule format: " + expression)
  }
}
object ScheduledMessage {
  val Nanos: Regex = """^([1-9]+\d*)$""".r
  //val Timestamp: Regex = """^(\d{4})-(\d{2})-(\d{2})(?:T(\d{2})(?::(\d{2})(?::(\d{2})(?:\.(\d{3}))?)?)?)?((\+|-)(\d{2}):(\d{2}))?$""".r
  val Timestamp: Regex = """^(\d{4})-(\d{2})-(\d{2})(?:T(\d{2})(?::(\d{2})(?::(\d{2})(?:\.(\d{3}))?)?)?)?([+-]\d{1,2}:\d{2})?$""".r
}

trait SchedulingMessage
case class Schedule(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String) extends SchedulingMessage
case class Cancel(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String) extends SchedulingMessage
case object Tick extends SchedulingMessage

object ScheduledMessage

class ScheduledMessageProvider extends Actor {
  val queue: ScheduledMessageQueue = InMemoryScheduledMessageQueue

  def receive = {
    case Schedule(sender, receiver, message, expression) =>
      queue.enqueue(sender, receiver, message, expression)
    case Cancel(sender, receiver, message, expression) =>
      queue.cancel(sender, receiver, message, expression)
    case Tick =>
      queue.dequeue().foreach {
        case ScheduledMessage(sender, receiver, message, _) =>
          receiver.tell(message, sender.orNull)
      }
  }
}

trait ScheduledMessageQueue {
  def enqueue(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String): Cancellable
  def peek(): Iterable[ScheduledMessage] = peek(System.currentTimeMillis())
  def peek(timestamp: Long): Iterable[ScheduledMessage]
  def dequeue(): Iterable[ScheduledMessage] = dequeue(System.currentTimeMillis())
  def dequeue(timestamp: Long): Iterable[ScheduledMessage]
  def cancel(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String)
}
object InMemoryScheduledMessageQueue extends ScheduledMessageQueue {
  var queue = SortedMap[Long, ScheduledMessage]()

  def enqueue(sender: Option[ActorRef], receiver: ActorRef, message: Any, expression: String) = {
    val sm = ScheduledMessage(sender, receiver, message, expression)
    queue += sm.nextOccurrence.getOrElse(sys.error("Scheduled message has no next occurrence: " + sm)) -> sm
    new Cancellable {
      val cancelled = new AtomicBoolean(false)

      def isCancelled: Boolean = cancelled.get()

      def cancel(): Boolean = {
        //println("cancel: " + sender + ", " + receiver + ", " + message + ", " + expression)
        InMemoryScheduledMessageQueue.this.cancel(sender, receiver, message, expression)
        true
      }
    }
  }

  def peek(timestamp: Long): Iterable[ScheduledMessage] = queue.until(timestamp).values

  def dequeue(timestamp: Long): Iterable[ScheduledMessage] = {
    val messages = peek(timestamp)
    queue = queue.from(timestamp)
    queue ++= messages.map { sm => sm.nextOccurrence -> sm }.collect { case (Some(t), sm) => t -> sm }
    messages
  }

  def cancel(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String) {
    val sm = ScheduledMessage(sender, receiver, message, expression)
    queue = queue.filterNot{ t =>
      //println("cancel: " + t)
      t._2 == sm
    }
  }
}


class SchedulingExtension(system: ExtendedActorSystem) extends Extension {
  private val queueRef = new AtomicReference[Option[ActorRef]](None)

  /**
   * Store associated with this extension.
   */
  def queue: ActorRef = queueRef.get.getOrElse(throw new IllegalStateException("No queue registered"))

  private [scheduling] def registerStore(queue: ActorRef) {
    queueRef.set(Option(queue))
  }

}

/**
 * Scheduling extension access point.
 */
object SchedulingExtension extends ExtensionId[SchedulingExtension] with ExtensionIdProvider {

  /**
   * Obtains the `SchedulingExtension` instance associated with `system`, registers a `queue`
   * on that instance and returns it.
   *
   * @param system actor system associated with the returned extension instance.
   * @param queue queue to register.
   */
  def apply(system: ActorSystem, queue: ActorRef): SchedulingExtension = {
    val extension = super.apply(system)
    extension.registerStore(queue)
    extension
  }

  def createExtension(system: ExtendedActorSystem) = new SchedulingExtension(system)

  def lookup() = SchedulingExtension
}