akkax-scheduling
================

Timestamp based persistent (durable) scheduling for [Akka] (http://akka.io). Timestamps can be provided in milliseconds (since Unix epoch) or as a date time string formatted according to [ISO-8601] (http://en.wikipedia.org/wiki/ISO_8601) (e.g. 2013-04-01T09:30:00+0200).
To schedule a message to an actor the following syntax is provided:
```scala
    // Set up an actor system
    implicit val system = ActorSystem("FooSystem")
    import system._

    // Set up the scheduling extension
    val scheduling = SchedulingExtension(system, new MapDBSchedulingQueue(new File("./.akkax-scheduling.db")))
    import scheduling._

    val actor = system.actorOf(...)

    // Schedule the message "foo" to be sent to the actor at the specified timestamp
    actor !@ "foo" -> "2013-06-01T10:30:00"

    // or equivalent
    actor.tellAt("foo", "2013-06-01T10:30:00")
```

Three scheduling queue implementations are provided:
* MemorySchedulingQueue - In memory queue which is not durable
* MapDBSchedulingQueue - Backed by a [MapDB] (http://www.mapdb.org/) map (made durable via the file system)
* SqlSchedulingQueue - Backed by a SQL capable database (using [Prequel] (https://github.com/jpersson/prequel))

To implement your own scheduling queue you need to implement the SchedulingQueue trait.