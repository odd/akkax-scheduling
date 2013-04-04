package akkax.scheduling
package sql

import java.io._
import collection.Iterable
import akka.actor.Cancellable
import net.noerd.prequel.{Nullable, NullComparable, DatabaseConfig, Formattable}
import net.noerd.prequel.SQLFormatterImplicits._
import org.joda.time.LocalDateTime
import scala.util.hashing.MurmurHash3

class SqlSchedulingQueue(driver: Option[String] = None, jdbcUrl: Option[String] = None) extends SchedulingQueue {
  trait Status
  case object Pending extends Status
  case object Resolved extends Status
  case object Cancelled extends Status
  case object Failed extends Status

  val database = DatabaseConfig(
    driver = driver.getOrElse("org.hsqldb.jdbc.JDBCDriver"),
    jdbcURL = jdbcUrl.getOrElse("jdbc:hsqldb:mem:mymemdb"))

  def withTableCreation(): SqlSchedulingQueue = {
    database.transaction { tx =>
      tx.execute("create table ScheduledMessage (" +
        "created char(23) not null, " +
        "senderId varchar(255) not null, " +
        "receiverId varchar(255) not null, " +
        "messageBytes varbinary(4096) not null, " +
        "messageHash int not null, " +
        "expression varchar(255) not null, " +
        "timestamp bigint not null, " +
        "status varchar(255) not null, " +
        "primary key (senderId, receiverId, messageBytes, expression))")
    }
    this
  }


  def enqueue(time: Long, sm: ScheduledMessage): Option[Cancellable] = {
    assert(sm.receiverId != null, s"Receiver id must not be null [$sm].")
    assert(sm.message != null, s"Message must not be null [$sm].")
    assert(sm.expression != null, s"Expression must not be null [$sm].")
    val bytes = serialize(sm.message)
    val hash = MurmurHash3.bytesHash(bytes)
    database.transaction { tx =>
      tx.execute(
        "insert into ScheduledMessage(created, senderId, receiverId, messageBytes, messageHash, expression, timestamp, status) values (?, ?, ?, ?, ?, ?, ?, ?)",
        new LocalDateTime().toString("yyyy-MM-dd'T'HH:mm:ss.SSS"), sm.senderId.getOrElse("").toString, sm.receiverId, bytes, hash, sm.expression, time, Pending.toString)
      None
    }
  }

  def peek(time: Long): Iterable[ScheduledMessage] = {
    database.transaction { tx =>
      tx.select("select senderId, receiverId, messageBytes, expression from ScheduledMessage where status = ? and timestamp < ? order by timestamp asc", Pending.toString, time) { r =>
        val senderId: Option[String] = r.nextString
        val receiverId: String = r.nextString.get
        val bytes: Array[Byte] = r.nextBinary.get
        val message: Any = deserialize(bytes)
        val expression: String = r.nextString.get

        ScheduledMessage(senderId, receiverId, message, expression)
      }
    }
  }

  def dequeue(time: Long) = {
    database.transaction { tx =>
      val messages = tx.select("select senderId, receiverId, messageBytes, expression from ScheduledMessage where status = ? and timestamp < ? order by timestamp", Pending.toString, time) { r =>
        val senderId: Option[String] = r.nextString.filter(_ != "")
        val receiverId: String = r.nextString.get
        val bytes: Array[Byte] = r.nextBinary.get
        val message: Any = deserialize(bytes)
        val expression: String = r.nextString.get

        ScheduledMessage(senderId, receiverId, message, expression)
      }
      val count = tx.execute("update ScheduledMessage set status = ? where status = ? and timestamp < ?", Resolved.toString, Pending.toString, time)
      assert(count == messages.size)
      messages
    }
  }

  def cancel(sm: ScheduledMessage) {
    database.transaction { tx =>
      val bytes = serialize(sm.message)
      val hash = MurmurHash3.bytesHash(bytes)
      val status = Cancelled.toString
      val senderId = sm.senderId.getOrElse("").toString
      val receiverId = sm.receiverId
      val expression = sm.expression
      val count = tx.execute("update ScheduledMessage set status = ? where senderId = ? and receiverId = ? and messageHash = ? and expression = ?", status, senderId, receiverId, hash, expression)
      if (count != 1) println(s"More than one scheduled message cancelled by message hash [senderId: $senderId, receiverId: $receiverId, messageHash: $hash, expression: $expression].")
      ()
    }
  }

  private [this] def serialize(o: Any): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes)
    try {
      out.writeObject(o)
    } finally {
      out.close()
    }
    bytes.toByteArray
  }

  private [this] def deserialize(bytes: Array[Byte]): Any = {
    val in = new ObjectInputStream(new ByteArrayInputStream(bytes))
    try {
      in.readObject()
    } finally {
      in.close()
    }
  }
}