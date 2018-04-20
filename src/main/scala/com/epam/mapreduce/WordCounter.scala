package com.epam.mapreduce

import com.epam.comparator.TextLengthComparator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

/*
 * Driver object
 */
object WordCounter {
  def main(args: Array[String]): Unit = {
    val configuration = new Configuration

    val job = Job.getInstance(configuration,"word count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[WordCountMapper])
    job.setCombinerClass(classOf[WordCountReducer])
    job.setReducerClass(classOf[WordCountReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job.setSortComparatorClass(classOf[TextLengthComparator])

    FileInputFormat.addInputPath(job, new Path(args(0)))

    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[IntWritable, Text]])
    if(job.waitForCompletion(true)) 0 else 1
  }
}
