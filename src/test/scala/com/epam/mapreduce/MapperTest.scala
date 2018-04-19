package com.epam.mapreduce

import org.apache.hadoop.io._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

class MapperTest extends FlatSpec with MockitoSugar {
  "map" should "output the words split on spaces" in {
    val mapper = new WordCountMapper
    val context = mock[mapper.Context]

    mapper.map(
      key = null,
      value = new Text("foo bar foo"),
      context
    )

    verify(context, times(2)).write(new Text("foo"), new IntWritable(1))
    verify(context).write(new Text("bar"), new IntWritable(1))
  }
  "map" should "output remove non-characters" in {
    val mapper = new WordCountMapper
    val context = mock[mapper.Context]

    mapper.map(
      key = null,
      value = new Text("foo! bar .foo"),
      context
    )

    verify(context, times(2)).write(new Text("foo"), new IntWritable(1))
    verify(context).write(new Text("bar"), new IntWritable(1))
  }
}

