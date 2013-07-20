package test

import akka.actor._
import akka.serialization.Serialization
import java.io.{ObjectInputStream, ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}

object ActorRefSerializationTest {
  def main(args: Array[String]) {
    implicit val system = ActorSystem("test")
    import system._

    println("La")
    val bytes1 = new ByteArrayOutputStream()
    val ref1 = system.actorOf(Props[LoggingActor])
    val identifier1: String = ref1.path.toString
    val out = new ObjectOutputStream(bytes1)
    out.writeObject(identifier1)
    out.close()

    val bytes2 = new ByteArrayInputStream(bytes1.toByteArray)
    val in = new ObjectInputStream(bytes2)
    val identifier2 = in.readObject().asInstanceOf[String]
    println(s"$identifier1 - $identifier2")
    assert(identifier1 == identifier2)
    val ref2 = system.actorSelection(identifier2)
    println(s"$ref1 - $ref2")
    assert(ref1 == ref2)

    system.shutdown()
  }
}

class LoggingActor extends Actor {
  def receive = {
    case x => println(x)
  }
}

object ExternalAddress extends ExtensionKey[ExternalAddressExt]

class ExternalAddressExt(system: ExtendedActorSystem) extends Extension {
  def addressForAkka: Address = system.provider.getDefaultAddress
}
