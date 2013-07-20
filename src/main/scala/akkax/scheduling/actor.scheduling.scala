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
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

case class ScheduledMessage(senderPath: Option[ActorPath] = None, receiverPath: ActorPath, message: Any, expression: String) {
  def sender(implicit system: ActorSystem): Option[ActorRef] = senderPath.map(system.actorSelection).map(actorRef)
  def receiver(implicit system: ActorSystem): ActorSelection = system.actorSelection(receiverPath)
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
  def actorRef(selection: ActorSelection)(implicit timeout: Timeout = Timeout(5 seconds)): ActorRef = {
     import akka.pattern.ask
     val id = selection.toString
     Await.result((selection ? Identify(id)).mapTo[ActorIdentity], timeout.duration) match {
       case ActorIdentity(`id`, Some(ref)) => ref
       case _ => null
     }
   }
}
object ScheduledMessage {
  val Nanos: Regex = """^([1-9]+\d*)$""".r
  val Timestamp: Regex = """^(\d{4,})-(\d{2})-(\d{2})(?:T(\d{2})(?::(\d{2})(?::(\d{2})(?:\.(\d{3}))?)?)?)?([+-]\d{1,2}:\d{2})?$""".r

  def apply(receiver: ActorRef, message: Any, expression: String): ScheduledMessage = apply(None, receiver, message, expression)
  def apply(sender: Option[ActorRef], receiver: ActorRef, message: Any, expression: String): ScheduledMessage = {
    val senderPath: Option[ActorPath] = sender.map(_.path)
    val receiverPath: ActorPath = receiver.path
    new ScheduledMessage(senderPath, receiverPath, message, expression)
  }
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
