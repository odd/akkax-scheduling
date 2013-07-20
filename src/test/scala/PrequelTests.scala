import net.noerd.prequel.{SQLFormatter, Formattable, DatabaseConfig}
import org.scalatest.FunSuite

class PrequelTests extends FunSuite {
  val string = "Kalle"
  val bytes = string.getBytes

  import net.noerd.prequel.SQLFormatterImplicits.{string2Formattable, binary2Formattable}

  test("Binary data is handled correctly") {
    val hsql = DatabaseConfig(
        driver = "org.hsqldb.jdbc.JDBCDriver",
        jdbcURL = "jdbc:hsqldb:mem:mymemdb")
    val mysql = DatabaseConfig(
      driver = "com.mysql.jdbc.Driver",
      jdbcURL = "jdbc:mysql://localhost:3306/test_prequel?profileSQL=false&createDatabaseIfNotExist=true")
    val database = mysql

    database.transaction { tx =>
      tx.execute("drop table if exists foo")
      tx.execute("create table foo (string varchar(255) not null, bytes varbinary(255) not null)")
      tx.executeBatch("insert into foo(string, bytes) values (?, ?)") { statement =>
        statement.executeWith(string, bytes)
      }
      tx.execute("insert into foo(string, bytes) values (?, ?)", string, bytes)
      tx.select("select string, bytes from foo") { r =>
        assert(r.nextString.get === string)
        assert(r.nextBinary.get === bytes)
      }
    }
  }
}