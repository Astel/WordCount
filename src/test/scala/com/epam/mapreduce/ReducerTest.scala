package com.epam.mapreduce

import org.apache.hadoop.io._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class ReducerTest extends FlatSpec with MockitoSugar {
  it should "output the word and the sum of its occurrences" in {
    val reducer = new WordCountReducer
    val context = mock[reducer.Context]

    reducer.reduce(
      key = new IntWritable(1),
      values = Seq(new Text("a"), new Text("b"), new Text("c")).asJava,
      context
    )

    verify(context).write(new IntWritable(1), new Text("a b c"))
  }
}
