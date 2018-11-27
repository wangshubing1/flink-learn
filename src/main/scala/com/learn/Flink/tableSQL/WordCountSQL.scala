package com.learn.Flink.tableSQL

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

/**
  * @Author: king
  * @Datetime: 2018/10/24
  * @Desc: TODO
  *
  */
object WordCountSQL {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    tEnv.registerDataSet("WordCount", input)
    val table = tEnv.sqlQuery("SELECT word, SUM(frequency) FROM WordCount GROUP BY word")

    table.toDataSet[WC].print()
  }
  case class WC(word: String, frequency: Long)

}
