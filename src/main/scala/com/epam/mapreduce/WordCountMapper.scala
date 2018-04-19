package com.epam.mapreduce

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class WordCountMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  override def map(
                    key: LongWritable,
                    value: Text,
                    context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    val words = value.toString.split("\\s+")

    words.map(word => word.replaceAll("[^A-Za-z0-9-]", ""))
      .foreach(word => {
        val key = new Text(word)
        val value = new IntWritable(1)
        context.write(key, value)
      })
  }
}
