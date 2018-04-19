package com.epam.it

import com.epam.mapreduce.WordCounter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest._

import scala.io.Source

class WordCounterIntegrationTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  "WordCount" should "write out word counts to output folder" in {
    WordCounter.main(Array())

    val output = Source.fromFile("output/part-r-00000").mkString

    output should equal(
      """|Bye	1
         |Goodbye	1
         |Hadoop	2
         |Hello	2
         |World	2
         |""".stripMargin)
  }

  override def afterEach = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path("output"), true)
  }
}
