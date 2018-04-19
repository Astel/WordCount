package com.epam.mapreduce

import org.apache.hadoop.io._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class ReducerTest extends FlatSpec with MockitoSugar {
  "reduce" should "output the word and the sum of its occurrences" in {
    val reducer = new WordCountReducer
    val context = mock[reducer.Context]

    reducer.reduce(
      key = new Text("one"),
      values = Seq(new IntWritable(1), new IntWritable(1)).asJava,
      context
    )

    verify(context).write(new Text("one"), new IntWritable(2))
  }
}
