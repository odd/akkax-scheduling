package akkax.scheduling

import akka.actor._
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class SchedulingExtension(system: ExtendedActorSystem) extends Extension {
  private val queueRef = new AtomicReference[Option[SchedulingQueue]](None)
  private val workerCancellableRef = new AtomicReference[Option[Cancellable]](None)

  private [scheduling] def queue: SchedulingQueue = {
    queueRef.get.getOrElse(throw new IllegalStateException("No scheduling queue registered."))
  }

  private [scheduling] def registerQueue(queue: SchedulingQueue, validator: ScheduledMessage => Boolean) {
    val o = Option(queue)
    queueRef.set(o)

    workerCancellableRef.get.foreach { ref =>
      ref.cancel
    }
    workerCancellableRef.set(None)

    o foreach { q =>
      import system._
      val worker = system.actorOf(Props(new SchedulingWorker(q, validator)))
      workerCancellableRef.set(Some(system.scheduler.schedule(
        initialDelay = Duration.create(1, TimeUnit.SECONDS),
        interval = Duration.create(1, TimeUnit.SECONDS),
        receiver = worker,
        message = SchedulingWorker.Tick)))
    }
  }

  implicit def enrichActorRef(actorRef: ActorRef)(implicit system: ActorSystem): SchedulingActorRef = new SchedulingActorRef(actorRef, system)

  def schedule(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String): Cancellable = {
    queue.enqueue(sender, receiver, message, expression)
  }
}

object SchedulingExtension extends ExtensionId[SchedulingExtension] with ExtensionIdProvider {
  def apply(system: ActorSystem, queue: SchedulingQueue, validator: ScheduledMessage => Boolean = { _ => true }): SchedulingExtension = {
    val extension = super.apply(system)
    extension.registerQueue(queue, validator)
    extension
  }
  def createExtension(system: ExtendedActorSystem) = new SchedulingExtension(system)
  def lookup() = SchedulingExtension
}

class SchedulingActorRef(actorRef: ActorRef, system: ActorSystem) {
  def !@(messageWithExpression: Pair[Any, String])(implicit sender: ActorRef = null): Cancellable = {
    SchedulingExtension(system).schedule(sender = Option(sender), receiver = actorRef, message = messageWithExpression._1, expression = messageWithExpression._2)
  }
  def tellAt(message: Any, expression: String)(): Cancellable = this.!@((message, expression))(null: ActorRef)
  def tellAt(message: Any, expression: String, sender: ActorRef): Cancellable = this.!@((message, expression))(sender)
}

object SchedulingWorker {
  trait SchedulingMessage
  case class Schedule(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String) extends SchedulingMessage
  case class Cancel(sender: Option[ActorRef] = None, receiver: ActorRef, message: Any, expression: String) extends SchedulingMessage
  case object Tick extends SchedulingMessage
}

class SchedulingWorker(queue: SchedulingQueue, validator: ScheduledMessage => Boolean) extends Actor {
  import SchedulingWorker._
  def receive = {
    case Schedule(sender, receiver, message, expression) =>
      queue.enqueue(sender, receiver, message, expression)
    case Cancel(sender, receiver, message, expression) =>
      queue.cancel(sender, receiver, message, expression)
    case Tick =>
      queue.dequeue().foreach {
        case sm: ScheduledMessage if (validator(sm)) =>
          implicit val system = context.system
          sm.receiver.tell(sm.message, sm.sender.orNull)
        case sm => println("Non valid message ignored [" + sm + "].")
      }
  }
}

