package akka.contrib
package persistence.scheduling

import scala.concurrent.duration.{Duration, FiniteDuration}
import akka.pattern.ask
import akka.actor._
import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace
import java.util.{Calendar, TimeZone, GregorianCalendar}
import scala.util.matching.Regex
import akka.contrib.persistence.scheduling.Schedules.Singular
import akka.persistence.{SnapshotOffer, EventsourcedProcessor}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

case class PersistentSchedulerException(msg: String) extends akka.AkkaException(msg) with NoStackTrace

/**
 * A persistent Akka scheduler service.
 */
trait PersistentScheduler {
  private[this] def now: Long = System.currentTimeMillis()

  /**
   * Schedules a message to be sent repeatedly with an initial delay and
   * frequency. E.g. if you would like a message to be sent immediately and
   * thereafter every 500ms you would set delay=Duration.Zero and
   * interval=Duration(500, TimeUnit.MILLISECONDS)
   *
   * Java & Scala API
   */
  final def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration,
    receiver: ActorRef,
    message: Any)(implicit executor: ExecutionContext,
                  sender: ActorRef = Actor.noSender): Cancellable =
    schedule(Schedules.Interval(now + initialDelay.length, interval), receiver, message)

  def schedule(
    schedule: Schedule,
    receiver: ActorRef,
    message: Any)(implicit executor: ExecutionContext,
                  sender: ActorRef = Actor.noSender): Cancellable

  /**
   * Schedules a message to be sent once with a delay, i.e. a time period that has
   * to pass before the message is sent.
   *
   * Java & Scala API
   */
  final def scheduleOnce(
    delay: FiniteDuration,
    receiver: ActorRef,
    message: Any)(implicit executor: ExecutionContext,
                  sender: ActorRef = Actor.noSender): Cancellable =
    scheduleOnce(Schedules.Singular(now + delay.length), receiver, message)

  def scheduleOnce(
    schedule: Schedules.Singular,
    receiver: ActorRef,
    message: Any)(implicit executor: ExecutionContext,
                  sender: ActorRef = Actor.noSender): Cancellable

  /**
   * The maximum supported task frequency of this scheduler, i.e. the inverse
   * of the minimum time interval between executions of a recurring task, in Hz.
   */
  def maxFrequency: Double
}

// this one is just here so we can present a nice AbstractPersistentScheduler for Java
abstract class AbstractPersistentSchedulerBase extends PersistentScheduler

trait Schedule {
  def nextOccurrence: Option[Long]
}
object Schedules {

  case class Interval(startTime: Long, interval: FiniteDuration) extends Schedule {
    def nextOccurrence = {
      val now: Long = System.currentTimeMillis()
      Some(now + ((now - startTime) % interval.length))
    }
  }
  object Singular {
    val ISO: Regex = """^(\d{4,})-(\d{2})-(\d{2})(?:T(\d{2})(?::(\d{2})(?::(\d{2})(?:\.(\d{3}))?)?)?)?([+-]\d{1,2}:\d{2})?$""".r
    def apply(isoTimestamp: String): Singular = isoTimestamp match {
      case ISO(year, month, day, hour, minute, second, millis, timeZone) =>
        def int(str: String) = if (str == null || str == "") 0 else str.toInt
        val calendar = new GregorianCalendar(int(year), int(month) - 1, int(day), int(hour), int(minute), int(second))
        if (timeZone != null && timeZone != "") calendar.setTimeZone(TimeZone.getTimeZone("GMT" + timeZone))
        calendar.set(Calendar.MILLISECOND, int(millis))
        Singular(calendar.getTimeInMillis)
      case _ =>
        sys.error("Unknown timestamp schedule format (expected ISO 8601): " + isoTimestamp)
    }
  }
  case class Singular(timestamp: Long) extends Schedule {
    def nextOccurrence: Option[Long] = {
      if (timestamp >= System.currentTimeMillis()) Some(timestamp)
      else None
    }
  }
  case class Cron(expression: String) extends Schedule {
    def nextOccurrence = ???
  }
}

class DefaultPersistentScheduler(implicit system: ActorSystem) extends PersistentScheduler {
  import Scheduler._

  private[this] val scheduler: ActorRef = system.actorOf(Props[Scheduler])

  /**
   * The maximum supported task frequency of this scheduler, i.e. the inverse
   * of the minimum time interval between executions of a recurring task, in Hz.
   */
  def maxFrequency = 1000L

  def scheduleOnce(s: Singular, receiver: ActorRef, message: Any)
                  (implicit executor: ExecutionContext, sender: ActorRef = Actor.noSender) = 
    schedule(s, receiver, message)

  def schedule(s: Schedule, receiver: ActorRef, message: Any)(implicit executor: ExecutionContext, sender: ActorRef): Cancellable = {
    val f = (scheduler ? Register(s, receiver, message, sender)).mapTo[Long]
    new Cancellable {
      private[this] val cancelled = new AtomicBoolean(false)

      def cancel(): Boolean = {
        if (cancelled.get) false
        else {
          f.onSuccess {
            case id => scheduler ! Cancel(id)
          }
          cancelled.set(true)
          true
        }
      }

      def isCancelled: Boolean = cancelled.get
    }
  }
}

private[scheduling] object Scheduler {
  type Id = Long
  trait Command
  trait Event {
    def id: Long
  }
  case class Register(schedule: Schedule, receiver: ActorRef, message: Any, sender: ActorRef) extends Command
  case class Cancel(id: Id) extends Command
  case class Registered(id: Id, schedule: Schedule, receiver: ActorRef, message: Any, sender: Option[ActorRef] = None) extends Event
  case class Cancelled(id: Id) extends Event
  case class State(maxId: Long = 0L, schedules: Map[Id, Registered] = Map.empty) {
    def update(r: Registered) = copy(maxId = math.max(maxId, r.id), schedules = schedules + (r.id -> r))
    def update(c: Cancelled) = copy(maxId = math.max(maxId, c.id), schedules = schedules - c.id)
    def nextId() = maxId + 1
    override def toString: String = schedules.mkString("Schedules (max id: " + maxId + "):\n", "\n\t", "\n")
  }
}
private[scheduling] class Scheduler(snapshotInterval: Duration) extends EventsourcedProcessor with ActorLogging {
  import Scheduler._
  implicit val system = context.system

  private[this] var state = State()
  private[this] var nanos = System.nanoTime()
  private[this] var cancellables = Map.empty[Long, Cancellable]

  override def preStart() = {
    super.preStart()
    val now = System.currentTimeMillis()
    cancellables = state.schedules.collect {
      case (id, sm @ ScheduledMessage(ScheduledMessage(messageSender, receiver, message, expression, time))) if (time > System.currentTimeMillis()) =>
        (id, system.scheduler.scheduleOnce(Duration(time, TimeUnit.MILLISECONDS), receiver, message)(system.dispatcher, messageSender))
    }
    state = state.copy(schedules = state.schedules.filterKeys(cancellables.keySet))
  }

  def update(e: Event): Unit = e match {
    case r: Registered =>
      state = state.update(r)
      //if (System.nanoTime() - nanos > snapshotInterval.length) saveSnapshot()
    case c: Cancelled =>
      state = state.update(c)
  }

  val receiveReplay: Receive = {
    case e: Event => update(e)
    case SnapshotOffer(_, snapshot: State) => state = snapshot
  }

  val receiveCommand: Receive = {
    case r @ Register(messageSender, receiver, message, expression) =>
      persist(Registered(state.nextId(), r))(update)
    case c @ Cancel(id) =>
      persist(Cancelled(id))(update)
  }
}
