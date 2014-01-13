package akka.contrib
package persistence.scheduling

import scala.concurrent.duration.FiniteDuration
import akka.actor.{Scheduler, Cancellable, Actor, ActorRef}
import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace

case class PersistentSchedulerException(msg: String) extends akka.AkkaException(msg) with NoStackTrace

/**
 * A persistent Akka scheduler service.
 */
trait PersistentScheduler {
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
    schedule(initialDelay, interval, new Runnable {
      def run = {
        receiver ! message
        if (receiver.isTerminated)
          throw new PersistentSchedulerException("timer active for terminated actor")
      }
    })

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
    scheduleOnce(delay, new Runnable {
      override def run = receiver ! message
    })

  /**
   * The maximum supported task frequency of this scheduler, i.e. the inverse
   * of the minimum time interval between executions of a recurring task, in Hz.
   */
  def maxFrequency: Double
}

// this one is just here so we can present a nice AbstractPersistentScheduler for Java
abstract class AbstractPersistentSchedulerBase extends PersistentScheduler
