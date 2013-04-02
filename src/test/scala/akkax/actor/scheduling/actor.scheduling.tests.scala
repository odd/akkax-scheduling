package akkax.actor.scheduling

import org.scalatest.FunSuite
import scala.concurrent.duration.Duration
import akka.event.Logging
import akka.pattern._
import akka.actor.{ActorSystem, Props, Actor}
import java.util.concurrent.TimeUnit
import java.text.SimpleDateFormat
import akkax.actor.scheduling.RecordingActor.Fetch
import akkax.actor.scheduling.memory.MemoryScheduledMessageQueue

class SchedulingExtensionTests extends FunSuite {
  val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  implicit val timeout = akka.util.Timeout(1000L)
  implicit val system = ActorSystem("TestSystem")
  import system._

  val scheduling = SchedulingExtension(system, new MemoryScheduledMessageQueue)
  import scheduling._
  system.scheduler.schedule(
    initialDelay = Duration.create(10, TimeUnit.SECONDS),
    interval = Duration.create(1, TimeUnit.SECONDS),
    receiver = system.actorOf(Props[Worker]),
    message = Worker.Tick)

  test("scheduled messages are scheduled") {
    println(s"test: scheduled messages are scheduled")
    val actor = system.actorOf(Props[RecordingActor], name = "kalle")
    println(s"actor: $actor")
    actor !@ "one" -> laterLiteral(3)
    actor !@ "two" -> laterLiteral(1)
    actor !@ "three" -> laterMilliseconds(7).toString
    actor ! "four"

    Thread.sleep(10000L)

    (actor ? Fetch) foreach {
      case xs: List[String] => assert(List("four", "two", "one", "three") === xs)
      case x ⇒ sys.error("Unknown reply: " + x)
    }
  }

  /*
  test("scheduled messages can be cancelled") {
    val actor = system.actorOf(Props[RecordingActor])
    val one = actor !@ "one" -> laterLiteral(1)
    val two = actor !@ "two" -> laterLiteral(3)
    val three = actor !@ "three" -> laterLiteral(5)
    two.cancel()

    Thread.sleep(10000L)

    (actor ? Fetch) foreach {
      case xs: List[String] => assert(List("one", "three") === xs)
      case x ⇒ sys.error("Unknown reply: " + x)
    }
  }
  */

  def laterLiteral(seconds: Int) = formatter.format(laterMilliseconds(seconds))
  def laterMilliseconds(seconds: Int) = System.currentTimeMillis() + 1000 * seconds
}

class RecordingActor extends Actor {
  val log = Logging(context.system, this)
  var messages: List[Any] = Nil

  def receive = {
    case Fetch =>
      sender ! messages.reverse
    case msg =>
      messages ::= msg
      log.info("received message: " + msg)
  }
}

object RecordingActor {
  case object Fetch
}