package com.epam.mapreduce

import com.epam.counters.Counters
import org.apache.hadoop.io._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar
import org.apache.hadoop.mapreduce.Counter

class MapperTest extends FlatSpec with MockitoSugar {

  it should "output the words split on spaces" in {
    val mapper = new WordCountMapper
    val context = mock[mapper.Context]
    val counter = mock[Counter]
    when(context.getCounter(Counters.groupName, Counters.INPUT_WORDS.toString))
      .thenReturn(counter)

    mapper.map(
      key = null,
      value = new Text("foo bar foo"),
      context
    )

    verify(context, times(2)).write(new IntWritable(3), new Text("foo"))
    verify(context).write(new IntWritable(3), new Text("bar"))
    verify(counter, times(3)).increment(1)
  }

  it should "output remove non-characters" in {
    val mapper = new WordCountMapper
    val context = mock[mapper.Context]
    val counter = mock[Counter]
    when(context.getCounter(Counters.groupName, Counters.INPUT_WORDS.toString))
      .thenReturn(counter)

    mapper.map(
      key = null,
      value = new Text("foo! bar .foo"),
      context
    )

    verify(context, times(2)).write(new IntWritable(3), new Text("foo"))
    verify(context).write(new IntWritable(3), new Text("bar"))
    verify(counter, times(3)).increment(1)
  }
}

