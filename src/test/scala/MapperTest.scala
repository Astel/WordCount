import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.scalatest.{FunSpec, Matchers}
import com.epam.mapreduce.WordCounter._


class MapperTest extends FunSpec with Matchers {
  describe("WordCounter Mapper") {
    describe("create method") {
      it("should succeed when creating nonexistent user") {



}
/*it("should fail when creating user with existing email") {
  val invalidUserException = db.run(userDAO.create(users.head)).failed.futureValue
  invalidUserException shouldBe a[PSQLException]
}
*/
    }
  }
}

