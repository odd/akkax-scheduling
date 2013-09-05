package akkax.scheduling

import org.scalatest.FunSuite
import scala.concurrent.duration.Duration
import akka.event.Logging
import akka.pattern._
import akka.actor.{ActorSystem, Props, Actor}
import java.util.concurrent.TimeUnit
import java.text.SimpleDateFormat
import java.io.File
import mapdb.MapDBSchedulingQueue
import akkax.scheduling.memory.MemorySchedulingQueue
import akkax.scheduling.sql.SqlSchedulingQueue
import akka.testkit.TestActorRef

trait SchedulingTests { this: FunSuite =>
  def withQueue(createQueue: => SchedulingQueue) {
    val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    implicit val timeout = akka.util.Timeout(1000L)
    implicit val system = ActorSystem("TestSystem")
    import system._

    val queue = createQueue
    val scheduling = SchedulingExtension(system, queue)
    import scheduling._

    test(s"scheduled messages are scheduled [${queue.getClass.getSimpleName}]") {
      println(s"test: scheduled messages are scheduled")
      val actor = TestActorRef(new RecordingActor)
      actor ! "zero"
      Thread.sleep(1000L)

      actor !@ "one" -> laterLiteral(3)
      actor !@ "two" -> laterLiteral(1)
      actor !@ "three" -> laterMilliseconds(7).toString
      actor ! "four"

      Thread.sleep(10000L)

      (actor ? RecordingActor.Fetch) foreach {
        case xs: List[_] => assert(List("zero", "four", "two", "one", "three") === xs)
        case x ⇒ sys.error("Unknown reply: " + x)
      }
    }
    test(s"scheduled messages can be cancelled [${queue.getClass.getSimpleName}]") {
      val actor = TestActorRef(new RecordingActor)
      actor ! "zero"
      val one = actor !@ "one" -> laterLiteral(1)
      val two = actor !@ "two" -> laterLiteral(3)
      val three = actor !@ "three" -> laterLiteral(5)
      two.cancel()

      Thread.sleep(10000L)

      (actor ? RecordingActor.Fetch) foreach {
        case xs: List[_] => assert(List("zero", "one", "three") === xs)
        case x ⇒ sys.error("Unknown reply: " + x)
      }
    }

    def laterLiteral(seconds: Int) = formatter.format(laterMilliseconds(seconds))
    def laterMilliseconds(seconds: Int) = System.currentTimeMillis() + 1000 * seconds
  }
}

class SchedulingSuite extends FunSuite with SchedulingTests {
  //def queues = Seq(new MemorySchedulingQueue, new MapDBSchedulingQueue(new File("./akkax-scheduling.db")))
  //def queues = Seq(new SqlSchedulingQueue(Some("org.hsqldb.jdbc.JDBCDriver"), Some("jdbc:hsqldb:mem:mymemdb")).withTableCreation())
  def queues = Seq(new SqlSchedulingQueue(Some("com.mysql.jdbc.Driver"), Some("jdbc:mysql://localhost:3306/test_akka_scheduling?profileSQL=false&createDatabaseIfNotExist=true")).withTableCreation())
  //def queues = Seq(new MapDBSchedulingQueue(new File("./.akkax-scheduling.db")))
  //def queues = Seq()

  queues.foreach(q => testsFor(withQueue(q)))
}

class RecordingActor extends Actor {
  import RecordingActor._
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