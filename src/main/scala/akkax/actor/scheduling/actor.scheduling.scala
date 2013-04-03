package akkax.actor.scheduling

import collection._
import scala.util.matching.Regex
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}
import javax.naming.OperationNotSupportedException
import java.util.{TimeZone, Calendar, GregorianCalendar}
import akka.actor._
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import java.io.{ObjectOutput, ObjectInput, Externalizable}
import akka.serialization.Serialization

class RichActorRef(actorRef: ActorRef, system: ActorSystem) {
  def !@(messageWithExpression: Pair[Any, String])(implicit sender: ActorRef = null): Cancellable = {
    SchedulingExtension(system).schedule(sender = Option(sender), receiver = actorRef, message = messageWithExpression._1, expression = messageWithExpression._2)
  }

  def tellAt(message: Any, expression: String)(): Cancellable = this.!@((message, expression))(null: ActorRef)

  def tellAt(message: Any, expression: String, sender: ActorRef): Cancellable = this.!@((message, expression))(sender)
}

class SchedulingExtension(system: ExtendedActorSystem) extends Extension {
  private val queueRef = new AtomicReference[Option[ScheduledMessageQueue]](None)
  private val workerCancellableRef = new AtomicReference[Option[Cancellable]](None)

  private [scheduling] def queue: ScheduledMessageQueue = {
    queueRef.get.getOrElse(throw new IllegalStateException("No scheduled message queue registered."))
  }

  private [scheduling] def registerQueue(queue: ScheduledMessageQueue) {
    val o = Option(queue)
    queueRef.set(o)

    workerCancellableRef.get.foreach { ref =>
      ref.cancel
    }
    workerCancellableRef.set(None)

    o foreach { q =>
      import system._
      val worker = system.actorOf(Props(new SchedulingWorker(q)))
      workerCancellableRef.set(Some(system.scheduler.schedule(
        initialDelay = Duration.create(1, TimeUnit.SECONDS),
        interval = Duration.create(1, TimeUnit.SECONDS),
        receiver = worker,
        message = SchedulingWorker.Tick)))
    }
  }

  implicit def enrichActorRef(actorRef: ActorRef)(implicit system: ActorSystem): RichActorRef = new RichActorRef(actorRef, system)

  def schedule(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String): Cancellable = {
    queue.enqueue(sender, receiver, message, expression)
  }
}

object SchedulingWorker {
  trait SchedulingMessage
  case class Schedule(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String) extends SchedulingMessage
  case class Cancel(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String) extends SchedulingMessage
  case object Tick extends SchedulingMessage
}

class SchedulingWorker(queue: ScheduledMessageQueue) extends Actor {
  import SchedulingWorker._
  def receive = {
    case Schedule(sender, receiver, message, expression) =>
      queue.enqueue(sender, receiver, message, expression)
    case Cancel(sender, receiver, message, expression) =>
      queue.cancel(sender, receiver, message, expression)
    case Tick =>
      queue.dequeue().foreach {
        case sm: ScheduledMessage =>
          implicit val system = context.system
          sm.receiver.tell(sm.message, sm.sender.orNull)
      }
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
  def apply(system: ActorSystem, queue: ScheduledMessageQueue): SchedulingExtension = {
    val extension = super.apply(system)
    extension.registerQueue(queue)
    extension
  }

  def createExtension(system: ExtendedActorSystem) = new SchedulingExtension(system)

  def lookup() = SchedulingExtension

}

//case class ScheduledMessage(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String) {
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
  //val Timestamp: Regex = """^(\d{4})-(\d{2})-(\d{2})(?:T(\d{2})(?::(\d{2})(?::(\d{2})(?:\.(\d{3}))?)?)?)?((\+|-)(\d{2}):(\d{2}))?$""".r
  val Timestamp: Regex = """^(\d{4})-(\d{2})-(\d{2})(?:T(\d{2})(?::(\d{2})(?::(\d{2})(?:\.(\d{3}))?)?)?)?([+-]\d{1,2}:\d{2})?$""".r
}

trait ScheduledMessageQueue { self =>
  final def enqueue(sender: Option[ActorRef], receiver: ActorRef, message: Any, expression: String): Cancellable = {
    val sm = ScheduledMessage(sender, receiver, message, expression)
    val time = sm.nextOccurrence.getOrElse(sys.error("Scheduled message has no next occurrence: " + sm))
    val cancellable = enqueue(time, sm)
    enqueued(time, sm)
    cancellable.getOrElse(new Cancellable {
      val cancelled = new AtomicBoolean(false)

      def isCancelled: Boolean = cancelled.get()

      def cancel(): Boolean = {
        println(s"cancel: $sm")
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
