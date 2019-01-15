package com.king.learn.Flink.streaming.join

import com.king.learn.Flink.streaming.join.source.WindowJoinSampleData
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author: king
  * @Date: 2019-01-14
  * @Desc: TODO
  *       示例说明两个数据流之间的窗口化流连接。
  *
  *        该示例适用于具有对（名称，等级）和（名称，工资）的两个输入流
  *        分别。它在可配置窗口中基于“名称”加入流。
  *
  *        该示例使用生成的内置示例数据生成器
  *        以可配置的速率对的蒸汽。
  *
  */

object WindowJoin {
  // *************************************************************************
  //  Program Data Types
  // *************************************************************************

  case class Grade(name: String, grade: Int)

  case class Salary(name: String, salary: Int)

  case class Person(name: String, grade: Int, salary: Int)

  // *************************************************************************
  //  Program
  // *************************************************************************

  def main(args: Array[String]) {
    // 参数设置
    val params = ParameterTool.fromArgs(args)
    val windowSize = params.getLong("windowSize", 100)
    val rate = params.getLong("rate", 100)

    println("Using windowSize=" + windowSize + ", data rate=" + rate)
    println("To customize example, use: WindowJoin " +
      "[--windowSize <window-size-in-millis>] [--rate <elements-per-second>]")

    // 获取执行环境，在“摄取时间”中运行此示例
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    // 在Web界面中提供参数
    env.getConfig.setGlobalJobParameters(params)

    // 为成绩和薪水创建数据源
    val grades = WindowJoinSampleData.getGradeSource(env, rate)

    val salaries = WindowJoinSampleData.getSalarySource(env, rate)

    // 在窗口上按名称加入两个输入流。
    // 为了测试性，此功能在一个单独的方法中。
    val joined = joinStreams(grades, salaries, windowSize)

    // 使用单个线程打印结果，而不是并行打印
    joined.print().setParallelism(1)

    // 执行程序
    env.execute("Windowed Join Example")
  }


  def joinStreams(grades: DataStream[Grade], salaries: DataStream[Salary], windowSize: Long) : DataStream[Person] = {
    grades.join(salaries)
      .where(_.name)
      .equalTo(_.name)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
      .apply { (g, s) => Person(g.name, g.grade, s.salary) }
  }


}
