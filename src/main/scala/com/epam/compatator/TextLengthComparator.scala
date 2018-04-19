package com.epam.compatator

import org.apache.hadoop.io.{Text, WritableComparable, WritableComparator}

class TextLengthComparator extends WritableComparator {

  override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = {
    val key1 = w1.asInstanceOf[Text]
    val key2 = w2.asInstanceOf[Text]
    key1.getLength - key2.getLength
  }
}
