package akka.contrib
package persistence.scheduling

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.{Actor, Props, ActorSystem}
import org.scalatest.{Matchers, FunSuiteLike}
import concurrent.duration.Duration
import concurrent.duration._

class PersistentSchedulingSpec extends TestKit(ActorSystem("test")) with ImplicitSender with FunSuiteLike with Matchers {
  val persistentScheduling = PersistentScheduling(system)
  import persistentScheduling._

  def in(d: Duration) = (System.currentTimeMillis() + d.length).toString

  test("Test request 1 account type") {
    val recorder = system.actorOf(Props[Recorder])
    recorder !@ "2 second" -> in(2 second)
    recorder ! "1"
    recorder !@ "3 seconds" -> in(3 seconds)
    recorder !@ "1 second" -> in(1 second)
    recorder ! "2"
    recorder ! "3"

       
    receiveOne(10.seconds) match {
      case result: List[_] ⇒
        result should have size 1
      case result ⇒
        assert(false, s"Expect List, got ${result.getClass}")
    }
  }
}

object Recorder {
  case object Recording
}
class Recorder extends Actor {
  import Recorder._
  var messages: List[(Long, String)] = Nil
  def now = System.currentTimeMillis()
  def receive = {
    case msg: String => messages ::= now -> msg
    case Recording => sender ! messages
  }
}