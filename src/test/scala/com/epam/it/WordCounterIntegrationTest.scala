package com.epam.it

import com.epam.comparator.LengthComparator
import com.epam.mapreduce.{WordCountMapper, WordCountReducer, WordCounter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.{IntWritable, SequenceFile, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.scalatest._

class WordCounterIntegrationTest extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  it should "write out word counts to output folder" in {
    WordCounter.main(Array("input", "output"))
    val outputFilePath = new Path("output/part-r-00000")
    val output = new StringBuilder("")
    val reader = new SequenceFile.Reader(new Configuration(), Reader.file(outputFilePath))

    val key = new IntWritable()
    val value = new Text()

    while ( {
      reader.next(key, value)
    }) output.append(key + "\t" + value)

    output.mkString shouldEqual "7	Hadooop Goodbye"
  }

  it should "correct use combiner" in {
    val configuration = new Configuration

    val job = Job.getInstance(configuration,"word count")
    job.setJarByClass(WordCounter.getClass)
    job.setMapperClass(classOf[WordCountMapper])
    job.setCombinerClass(classOf[WordCountReducer])

    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[Text])

    job.setSortComparatorClass(classOf[LengthComparator])

    FileInputFormat.addInputPath(job, new Path("input"))
    FileOutputFormat.setOutputPath(job, new Path("output"))

    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[IntWritable, Text]])

    val result = job.waitForCompletion(true)

    val outputFilePath = new Path("output/part-r-00000")
    val output = new StringBuilder("")
    val reader = new SequenceFile.Reader(new Configuration(), Reader.file(outputFilePath))

    val key = new IntWritable()
    val value = new Text()

    while ( {
      reader.next(key, value)
    }) output.append(key + "\t" + value)

    output.mkString should equal("7\tHadooop Goodbye")
  }

  override def beforeAll(): Unit = {
    System.setProperty("hadoop.home.dir", "/")
    clearFileSystem
  }

  override def beforeEach(): Unit = {
    val fs = FileSystem.get(new Configuration())
    val inputPath = new Path(getClass.getResource("/testData.txt").getPath)
    fs.copyFromLocalFile(inputPath, new Path("input"))
  }

  override def afterEach: Unit = {
    clearFileSystem
  }

  private def clearFileSystem = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path("output"), true)
    fs.delete(new Path("input"), true)
  }
}
