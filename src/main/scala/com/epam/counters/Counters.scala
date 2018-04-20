package com.epam.counters

import com.epam.counters

/*
 * Custom counter for WordCounter
 */
object Counters extends Enumeration {
  val groupName = "WordCount"

  type Counters = Value
  val INPUT_WORDS: counters.Counters.Value = Value("Input words")
}
