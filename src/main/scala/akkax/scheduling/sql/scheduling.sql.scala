package akkax.scheduling
package sql

import java.io._
import collection.Iterable
import akka.actor.{ActorPath, Cancellable}
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
      tx.execute("create table if not exists ScheduledMessage (" +
        "created char(23) not null, " +
        "senderPath varchar(255) not null, " +
        "receiverPath varchar(255) not null, " +
        "messageBytes varbinary(4096) not null, " +
        "messageHash int not null, " +
        "expression varchar(255) not null, " +
        "timestamp bigint not null, " +
        "status varchar(255) not null, " +
        "primary key (senderPath, receiverPath, messageHash, expression))")
    }
    this
  }


  def enqueue(time: Long, sm: ScheduledMessage): Option[Cancellable] = {
    assert(sm.receiverPath != null, s"Receiver path must not be null [$sm].")
    assert(sm.message != null, s"Message must not be null [$sm].")
    assert(sm.expression != null, s"Expression must not be null [$sm].")
    val bytes = serialize(sm.message)
    val hash = MurmurHash3.bytesHash(bytes)
    println(s"!!! bytes with hash #$hash ('${sm.message}'): ${bytes.map(_.toInt).mkString(" ")}")
    database.transaction { tx =>
      tx.execute(
        "insert into ScheduledMessage(created, senderPath, receiverPath, messageBytes, messageHash, expression, timestamp, status) values (?, ?, ?, ?, ?, ?, ?, ?)",
        new LocalDateTime().toString("yyyy-MM-dd'T'HH:mm:ss.SSS"), sm.senderPath.map(_.toString).getOrElse("").toString, sm.receiverPath.toString, bytes, hash, sm.expression, time, Pending.toString)
      None
    }
  }

  def peek(time: Long): Iterable[ScheduledMessage] = {
    database.transaction { tx =>
      tx.select("select senderPath, receiverPath, messageBytes, messageHash, expression from ScheduledMessage where status = ? and timestamp < ? order by timestamp asc", Pending.toString, time) { r =>
        val senderPath: Option[ActorPath] = r.nextString.filter(_ != "").map(ActorPath.fromString)
        val receiverPath: ActorPath = ActorPath.fromString(r.nextString.get)
        val bytes: Array[Byte] = r.nextBinary.get
        val hash: Int = r.nextInt.get
        println(s"!!! bytes with hash #$hash: ${bytes.map(_.toInt).mkString(" ")}")
        val message: Any = deserialize(bytes)
        val expression: String = r.nextString.get

        ScheduledMessage(senderPath, receiverPath, message, expression)
      }
    }
  }

  def dequeue(time: Long) = {
    database.transaction { tx =>
      val messages = tx.select("select senderPath, receiverPath, messageBytes, messageHash, expression from ScheduledMessage where status = ? and timestamp < ? order by timestamp", Pending.toString, time) { r =>
        val senderPath: Option[ActorPath] = r.nextString.filter(_ != "").map(ActorPath.fromString)
        val receiverPath: ActorPath = ActorPath.fromString(r.nextString.get)
        val bytes: Array[Byte] = r.nextBinary.get
        val hash: Int = r.nextInt.get
        println(s"!!! bytes with hash #$hash: ${bytes.map(_.toInt).mkString(" ")}")
        val message: Any = deserialize(bytes)
        val expression: String = r.nextString.get

        ScheduledMessage(senderPath, receiverPath, message, expression)
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
      val senderPath = sm.senderPath.getOrElse("").toString
      val receiverPath = sm.receiverPath.toString
      val expression = sm.expression
      val count = tx.execute("update ScheduledMessage set status = ? where senderPath = ? and receiverPath = ? and messageHash = ? and expression = ?", status, senderPath, receiverPath, hash, expression)
      if (count != 1) println(s"More than one scheduled message cancelled by message hash [senderPath: $senderPath, receiverPath: $receiverPath, messageHash: $hash, expression: $expression].")
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