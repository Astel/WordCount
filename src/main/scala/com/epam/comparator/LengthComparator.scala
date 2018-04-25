package com.epam.comparator

import org.apache.hadoop.io.{IntWritable, Text, WritableComparable, WritableComparator}

/*
 * Sort text values by length
 */
class LengthComparator extends WritableComparator(classOf[IntWritable], true) {

  override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = {
    val key1 = w1.asInstanceOf[IntWritable]
    val key2 = w2.asInstanceOf[IntWritable]
    key2.get() - key1.get()
  }
}
