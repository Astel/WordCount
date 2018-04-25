package com.epam.mapreduce

import java.lang.Iterable

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._

class WordCountReducer extends Reducer[IntWritable, Text, IntWritable, Text] {
  var largestWordDone = false

  override def reduce(
      key: IntWritable,
      values: Iterable[Text],
      context: Reducer[IntWritable, Text, IntWritable, Text]#Context): Unit = {
    if(!largestWordDone) {
      largestWordDone = true
      val sum = values.asScala.fold(new Text(""))((a, b) => new Text(a + " " +  b))
      context.write(key, new Text(sum.toString.trim))
    }
  }
}
