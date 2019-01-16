package com.king.learn.Flink.batch.wordcount

import org.apache.flink.api.scala._

/**
  * @Author: king
  * @Date: 2019-01-16
  * @Desc: TODO 
  */

object WordCountExample {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?")

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.print()
  }
}
