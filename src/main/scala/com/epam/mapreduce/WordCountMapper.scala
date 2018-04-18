package com.epam.mapreduce

import java.util.StringTokenizer

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class WordCountMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val one = new IntWritable(1)
  val word = new Text

  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val itr = new StringTokenizer(value.toString)
    while (itr.hasMoreTokens) {
      word.set(itr nextToken)
      context.write(word, one)
    }
  }
}
