package akkax.scheduling
package eventsourced

import akka.persistence.{SnapshotOffer, EventsourcedProcessor}
import akka.actor.{ActorRef, ActorSystem, Cancellable, ActorLogging}

class EventSourcedSchedulingQueue(system: ActorSystem) extends SchedulingQueue { self =>
  import Persister._

  var persister: Option[ActorRef] = None

  def enqueue(time: Long, message: ScheduledMessage): Unit = {
    persister foreach(_ ! Schedule(time, message))
  }

  def dequeue(time: Long): Unit = {
    persister foreach(_ ! (time, scheduledMessage))
  }


  def peek(time: Long) = persister

  def cancel(scheduledMessage: ScheduledMessage) = ???
}


object Persister {
  case class Schedule(time: Long, message: ScheduledMessage)
  case class Schedule(time: Long, message: ScheduledMessage)
  case class Evt(data: String)

  case class State(events: List[String] = Nil) {
    def update(evt: Evt) = copy(evt.data :: events)
    def size = events.length
    override def toString: String = events.reverse.toString
  }
}
class Persister extends EventsourcedProcessor with ActorLogging {
  import Persister._

  var state = State()
  var jobs: Seq[Cancellable] = Seq.empty

  def updateState(event: Evt): Unit = {
    state = state.update(event)
  }
  
  def eventCount = state.size

  val receiveReplay: Receive = {
    case e: Evt =>
      updateState(e)
      state = state.update(e)
    case SnapshotOffer(_, snapshot: State) =>
      state = snapshot
  }

  val receiveCommand: Receive = {
    case Cmd(data) =>
      persist(Evt(s"${data}-${eventCount}"))(updateState)
      persist(Evt(s"${data}-${eventCount + 1}")) { event =>
        updateState(event)
        context.system.eventStream.publish(event)
        if (data == "foo") context.become(otherCommandHandler)
      }
    case "snap"  => saveSnapshot(state)
    case "print" => println(state)
  }
}
