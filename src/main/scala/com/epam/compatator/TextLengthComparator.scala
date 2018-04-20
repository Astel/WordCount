package com.epam.compatator

import org.apache.hadoop.io.{Text, WritableComparable, WritableComparator}

/*
 * Sort text values by length
 */
class TextLengthComparator extends WritableComparator(classOf[Text], true) {

  override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = {
    val key1 = w1.asInstanceOf[Text]
    val key2 = w2.asInstanceOf[Text]
    key2.getLength - key1.getLength
  }
}
