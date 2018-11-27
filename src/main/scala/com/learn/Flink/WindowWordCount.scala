package com.learn.Flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author: king
  * @Datetime: 2018/10/16 
  * @Desc: TODO 流式处理
  *
  */
object WindowWordCount {
  def main(args: Array[String]) {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //链接数据流
    val text = env.socketTextStream("10.234.7.107", 9001)
    //解析数组，分组，窗口汇总统计数
    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      //根据key分组
      .keyBy(0)
      //时间间隔
      .timeWindow(Time.seconds(5))
      .sum(1)
    //用一个线程来打印结果，并不是并行的
    counts.print().setParallelism(10)

    env.execute("Window Stream WordCount")
  }
}
