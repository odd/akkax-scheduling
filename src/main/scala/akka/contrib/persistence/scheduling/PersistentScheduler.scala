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
import akka.util.Timeout

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
  def schedule(
    schedule: Schedule,
    receiver: ActorRef,
    message: Any)(implicit executor: ExecutionContext,
                  timeout: Timeout,
                  sender: ActorRef = Actor.noSender): Cancellable

  def scheduleOnce(
    schedule: Schedules.Singular,
    receiver: ActorRef,
    message: Any)(implicit executor: ExecutionContext,
                  timeout: Timeout,
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
  def next: Option[Long]
}
object Schedules {
  case class Every(interval: FiniteDuration, first: Long = 0) extends Schedule {
    def next = {
      val now = System.currentTimeMillis()
      Some(now + ((now - first) % interval.length))
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
        At(calendar.getTimeInMillis)
      case _ =>
        sys.error("Unknown timestamp schedule format (expected ISO 8601): " + isoTimestamp)
    }
  }
  trait Singular extends Schedule {
    val time: Long
    def next: Option[Long] = {
      if (time >= System.currentTimeMillis()) Some(time)
      else None
    }
  }
  case class At(time: Long) extends Singular
  case class In(delay: Long) extends Singular {
    val time = System.currentTimeMillis() + delay
  }
  case class Cron(expression: String) extends Schedule {
    def next = ???
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
                  (implicit executor: ExecutionContext, timeout: Timeout, sender: ActorRef = Actor.noSender) =
    schedule(s, receiver, message)

  def schedule(s: Schedule, receiver: ActorRef, message: Any)(implicit executor: ExecutionContext, timeout: Timeout, sender: ActorRef = Actor.noSender): Cancellable = {
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
  case class Registered(id: Id, schedule: Schedule, receiver: ActorRef, message: Any, sender: ActorRef) extends Event
  case class Cancelled(id: Id) extends Event
  case class State(maxId: Long = 0L, schedules: Map[Id, Registered] = Map.empty) {
    def update(r: Registered) = copy(maxId = math.max(maxId, r.id), schedules = schedules + (r.id -> r))
    def update(c: Cancelled) = copy(maxId = math.max(maxId, c.id), schedules = schedules - c.id)
    def nextId() = maxId + 1
    override def toString: String = schedules.mkString("Schedules (max id: " + maxId + "):\n", "\n\t", "\n")
  }
  object Invocation {
    def unapply(r: Registered)(implicit system: ActorSystem): Option[(Id, Schedule, ActorRef, Any, ActorRef, Option[Long])] = {
      Some((r.id, r.schedule, r.receiver, r.message, r.sender, r.schedule.next))
    }
  }
}
private[scheduling] class Scheduler(snapshotInterval: Duration) extends EventsourcedProcessor with ActorLogging {
  import Scheduler._
  implicit val system = context.system

  private[this] var state = State()
  private[this] var nanos = System.nanoTime()
  private[this] var cancellables = Map.empty[Long, Cancellable]

  private[this] def now = System.currentTimeMillis()

  override def preStart() = {
    super.preStart()
    val now = System.currentTimeMillis()
    cancellables = state.schedules.collect {
      case (id, i @ Invocation(_, schedule, receiver, message, messageSender, Some(time))) if (time > now) =>
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
    case r @ Register(schedule, receiver, message, messageSender) =>
      val r2: Registered = Registered(state.nextId(), schedule, receiver, message, messageSender)
      persist(r2)(update)
      val Invocation(id: Long, _, _, _, _, Some(time)) = r2
      val c: Cancellable = system.scheduler.scheduleOnce(Duration(time, TimeUnit.MILLISECONDS), receiver, message)(system.dispatcher, messageSender)
      cancellables += id -> c
    case c @ Cancel(id) =>
      persist(Cancelled(id))(update)
      cancellables.get(id).foreach(_.cancel())
      cancellables -= id
  }
}
