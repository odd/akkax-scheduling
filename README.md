akkax-scheduling
================

Timestamp based persistent (durable) scheduling for [Akka] (http://akka.io). Timestamps can be provided in milliseconds (since Unix epoch) or as a date time string formatted according to [ISO-8601] (http://en.wikipedia.org/wiki/ISO_8601) (e.g. 2013-04-01T09:30:00+0200).
To schedule a message to an actor the following syntax is provided:
```scala
    // Set up actor system
    implicit val system = ActorSystem("TestSystem")
    import system._

    // Set up the scheduling extension
    val scheduling = SchedulingExtension(system, new MapDBMemoryScheduledMessageQueue(new File("./.akkax-scheduling.db")))
    import scheduling._

    val actor = system.actorOf(...)

    // Schedule the message "foo" to be sent to the actor at the specified timestamp
    actor !@ "foo" -> "2013-06-01T10:30:00"

    // or equivalent
    actor.tellAt("foo", "2013-06-01T10:30:00")
```

Three scheduled message queue implementations are provided:
* MemoryScheduledMessageQueue - In memory queue which is not durable
* MapDBScheduledMessageQueue - Backed by a MapDB map which is made durable via files
* SqlScheduledMessageQueue - Backed by any SQL capable database

To implement your own queue you need to implement the ScheduledMessageQueue trait.