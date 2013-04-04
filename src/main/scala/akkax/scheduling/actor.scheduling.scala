package akkax.scheduling

import collection._
import scala.util.matching.Regex
import java.io.{ObjectOutput, ObjectInput, Externalizable}
import java.util.{TimeZone, Calendar, GregorianCalendar}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}
import scala.concurrent.duration.Duration
import akka.actor._
import akka.serialization.Serialization

case class ScheduledMessage(@transient senderRef: Option[ActorRef] = None, @transient receiverRef: ActorRef, message: Any, expression: String) {
  val senderId: Option[String] = senderRef.map { ref =>
    Serialization.currentTransportAddress.value match {
        case null    ⇒ ref.path.toString
        case address ⇒ ref.path.toStringWithAddress(address)
    }
  }
  val receiverId: Option[String] = Option(receiverRef).map { ref =>
    Serialization.currentTransportAddress.value match {
      case null    ⇒ ref.path.toString
      case address ⇒ ref.path.toStringWithAddress(address)
    }
  }
  def sender(implicit system: ActorSystem): Option[ActorRef] = senderId.map(system.actorFor)
  def receiver(implicit system: ActorSystem): ActorRef = system.actorFor(receiverId.getOrElse(throw new IllegalStateException("Sentinel actor ref should never be used.")))
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
  val Timestamp: Regex = """^(\d{4,})-(\d{2})-(\d{2})(?:T(\d{2})(?::(\d{2})(?::(\d{2})(?:\.(\d{3}))?)?)?)?([+-]\d{1,2}:\d{2})?$""".r
}

trait SchedulingQueue { self =>
  final def enqueue(sender: Option[ActorRef], receiver: ActorRef, message: Any, expression: String): Cancellable = {
    val sm = ScheduledMessage(sender, receiver, message, expression)
    val time = sm.nextOccurrence.getOrElse(sys.error("Scheduled message has no next occurrence: " + sm))
    val cancellable = enqueue(time, sm)
    enqueued(time, sm)
    cancellable.getOrElse(new Cancellable {
      val cancelled = new AtomicBoolean(false)

      def isCancelled: Boolean = cancelled.get()

      def cancel(): Boolean = {
        self.cancel(sm)
        true
      }
    })
  }
  def enqueue(time: Long, scheduledMessage: ScheduledMessage): Option[Cancellable]
  def enqueued(time: Long, scheduledMessage: ScheduledMessage) = ()

  final def peek: Iterable[ScheduledMessage] = peek(System.currentTimeMillis())
  def peek(time: Long): Iterable[ScheduledMessage]

  final def dequeue(): Iterable[ScheduledMessage] = {
    val time = System.currentTimeMillis()
    val messages = dequeue(time)
    if (messages.nonEmpty) dequeued(time)
    messages
  }
  def dequeue(time: Long): Iterable[ScheduledMessage]
  def dequeued(time: Long) = ()

  final def cancel(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String): Unit = {
    cancel(ScheduledMessage(sender, receiver, message, expression))
  }
  def cancel(scheduledMessage: ScheduledMessage): Unit
  def cancelled(scheduledMessage: ScheduledMessage) = ()
}
