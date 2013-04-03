import java.util.concurrent.ConcurrentNavigableMap

object MapDBTests {
  def main(args: Array[String]) {
    import org.mapdb._

    // configure and open database using builder pattern.
    // all options are available with code auto-completion.
    val db = DBMaker.newFileDB(new java.io.File("testdb"))
                   .closeOnJvmShutdown()
                   .make();

    // open existing an collection (or create new)
    val map: ConcurrentNavigableMap[Int,String]  = db.getTreeMap("collectionName");

    map.put(1, "one");
    map.put(2, "two");
    // map.keySet() is now [1,2]

    db.commit();  //persist changes into disk

    map.replace(2, "TWO")
    db.commit();  //persist changes into disk

    map.put(3, "three");
    // map.keySet() is now [1,2,3]
    db.rollback(); //revert recent changes
    // map.keySet() is now [1,2]

    db.close();
  }
}
