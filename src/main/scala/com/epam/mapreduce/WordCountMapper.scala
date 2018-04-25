package com.epam.mapreduce

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import com.epam.counters.Counters

class WordCountMapper extends Mapper[LongWritable, Text, IntWritable, Text] {
  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, IntWritable, Text]#Context): Unit = {

    //split by spaces
    val words = value.toString.split("\\s+")
    val one = new IntWritable(1)
    val inputWordsCounter = context.getCounter(Counters.groupName, Counters.INPUT_WORDS.toString)

    words
      //delete all noncharacters, nondigits except "-" to clean the text
      .map(word => word.replaceAll("[^A-Za-z0-9-]", ""))
      .foreach(word => {
        inputWordsCounter.increment(1)
        val key = new IntWritable(word.length)
        val value = new Text(word)
        context.write(key, value)
      })
  }
}
