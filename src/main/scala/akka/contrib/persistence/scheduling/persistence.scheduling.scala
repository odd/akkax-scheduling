package akka.contrib
package persistence.scheduling

import akka.actor._
import akka.persistence.{SnapshotOffer, EventsourcedProcessor}
import akka.pattern.ask
import scala.concurrent.{ExecutionContext, Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import java.util.concurrent.TimeUnit
import scala.util.matching.Regex
import java.util.{Calendar, TimeZone, GregorianCalendar}
import akka.util.Timeout

object PersistentScheduling extends ExtensionId[PersistentScheduling] with ExtensionIdProvider {
  override def get(system: ActorSystem): PersistentScheduling = super.get(system)

  override def lookup = PersistentScheduling

  override def createExtension(system: ExtendedActorSystem): PersistentScheduling = new PersistentScheduling(system)
}

/**
 * @see [[PersistentScheduling$ PersistentScheduling companion object]]
 */
class PersistentScheduling(system: ExtendedActorSystem, snapshotInterval: Duration = Duration(10, TimeUnit.MINUTES)) extends Extension {
  import Scheduler._

  private[this] val scheduler = system.actorOf(Props[Scheduler](new Scheduler(snapshotInterval)))

  def schedule(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String)(implicit timeout: Timeout): Future[Long] = {
    (scheduler ? Schedule(sender, receiver, message, expression)).mapTo[Long]
  }

  def cancel(id: Long): Unit = {
    scheduler ! Cancel(id)
  }

  /**
   * Makes the tellAt and !@ methods available on all actor refs
   */
  implicit def enrichActorRef(actorRef: ActorRef)(implicit system: ActorSystem): PersistentSchedulingActorRef = new PersistentSchedulingActorRef(actorRef, system)
}

class PersistentScheduler extends akka.actor.Scheduler {
  def maxFrequency = Double

  def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(implicit executor: ExecutionContext) = ???

  def schedule(initialDelay: FiniteDuration, interval: FiniteDuration, runnable: Runnable)(implicit executor: ExecutionContext) = ???
}



object Scheduler {
  trait Command
  trait Event {
    def id: Long
  }
  object Schedule {
    val Nanos: Regex = """^([1-9]+\d*)$""".r
    val Timestamp: Regex = """^(\d{4,})-(\d{2})-(\d{2})(?:T(\d{2})(?::(\d{2})(?::(\d{2})(?:\.(\d{3}))?)?)?)?([+-]\d{1,2}:\d{2})?$""".r
  }
  case class Schedule(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String) extends Command {
    import Schedule._
    def nextOccurrence: Option[Long] = expression match {
      case Schedule.Nanos(millis) =>
        val time = millis.toLong
        if (time >= System.currentTimeMillis()) Some(time)
        else None
      case Schedule.Timestamp(year, month, day, hour, minute, second, millis, timeZone) =>
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
  case class Cancel(id: Long) extends Command
  case class Scheduled(id: Long, schedule: Schedule) extends Event
  case class Cancelled(id: Long) extends Event
  case class State(maxId: Long = 0L, schedules: Map[Long, Schedule] = Map.empty) {
    def update(s: Scheduled) = copy(maxId = math.max(maxId, s.id), schedules = schedules + (s.id -> s.schedule))
    def update(c: Cancelled) = copy(maxId = math.max(maxId, c.id), schedules = schedules - c.id)
    def nextId() = maxId + 1
    override def toString: String = schedules.mkString("Schedules (max id: " + maxId + "):\n", "\n\t", "\n")
  }
}
class Scheduler(snapshotInterval: Duration) extends EventsourcedProcessor with ActorLogging {
  import Scheduler._
  import context.dispatcher
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
    case s: Scheduled =>
      state = state.update(s)
      //if (System.nanoTime() - nanos > snapshotInterval.length) saveSnapshot()
    case c: Cancelled =>
      state = state.update(c)
  }

  val receiveReplay: Receive = {
    case e: Event => update(e)
    case SnapshotOffer(_, snapshot: State) => state = snapshot
  }

  val receiveCommand: Receive = {
    case s @ Schedule(messageSender, receiver, message, expression) =>
      persist(Scheduled(state.nextId(), s))(update)
    case c @ Cancel(id) =>
      persist(Cancelled(id))(update)
  }
}

class PersistentSchedulingActorRef(actorRef: ActorRef, system: ActorSystem) {
  import system.dispatcher

  def !@(messageWithExpression: Pair[Any, String])(implicit sender: ActorRef = null, timeout: Timeout): Cancellable = {
    val scheduler = PersistentScheduling(system)
    val f: Future[Long] = scheduler.schedule(sender = Option(sender), receiver = actorRef, message = messageWithExpression._1, expression = messageWithExpression._2)
    new Cancellable {
      var isCancelled = false
      def cancel(): Boolean = {
        if (isCancelled) false
        else {
          f.onSuccess {
            case id =>
              scheduler.cancel(id)
              isCancelled = true
          }
          true
        }
      }
    }
  }

  def tellAt(message: Any, expression: String): Cancellable = tellAt(message, expression, null)
  def tellAt(message: Any, expression: String, sender: ActorRef): Cancellable = tellAt(message, expression, sender)
  def tellAt(message: Any, expression: String, sender: ActorRef, timeout: Timeout): Cancellable = this.!@((message, expression))(sender, timeout)
}

object ScheduledMessage {
  import Scheduler._

  def apply(receiver: ActorRef, message: Any, expression: String, time: Long): ScheduledMessage = apply(None, receiver, message, expression, time)
  def apply(sender: Option[ActorRef], receiver: ActorRef, message: Any, expression: String, time: Long): ScheduledMessage = {
    val senderPath: Option[ActorPath] = sender.map(_.path)
    val receiverPath: ActorPath = receiver.path
    new ScheduledMessage(senderPath, receiverPath, message, expression, time)
  }

  def unapply(sm: ScheduledMessage)(implicit system: ActorSystem): Option[(Option[ActorRef], ActorRef, Any, String, Long)] = {
    Some((sm.sender, sm.receiver, sm.message, sm.expression, sm.time))
  }
  def unapply(s: Schedule): Option[ScheduledMessage] = s match {
    case s @ Schedule(messageSender, receiver, message, expression) =>
      s.nextOccurrence.fold(None: Option[ScheduledMessage])(time => Some(ScheduledMessage(messageSender, receiver, message, expression, time)))
  }
}
class ScheduledMessage private (val senderPath: Option[ActorPath] = None, val receiverPath: ActorPath, val message: Any, val expression: String, val time: Long) {
  def sender(implicit system: ActorSystem): Option[ActorRef] = senderPath.map(system.actorSelection).map(actorRef)
  def receiver(implicit system: ActorSystem): ActorRef = Some(receiverPath).map(system.actorSelection).map(actorRef).get
  def actorRef(selection: ActorSelection)(implicit timeout: Timeout = Timeout(5 seconds)): ActorRef = {
     import akka.pattern.ask
     val id = selection.toString
     Await.result((selection ? Identify(id)).mapTo[ActorIdentity], timeout.duration) match {
       case ActorIdentity(`id`, Some(ref)) => ref
       case _ => null
     }
   }
}