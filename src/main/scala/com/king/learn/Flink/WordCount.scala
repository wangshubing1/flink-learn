package com.king.learn.Flink

import org.apache.flink.api.scala._
import org.apache.hadoop.io.{IntWritable, Text}


/**
  * @Author: king
  * @Datetime: 2018/10/16 
  * @Desc: TODO 批处理
  *
  */
object WordCount {
  def main(args: Array[String]) {

    //val env = ExecutionEnvironment.getExecutionEnvironment
    val env = ExecutionEnvironment.createLocalEnvironment()
    val text = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?")


    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .groupBy(0)
      .sum(1)

    counts.print()
  }
}
