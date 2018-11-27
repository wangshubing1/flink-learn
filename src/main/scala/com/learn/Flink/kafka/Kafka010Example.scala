package com.learn.Flink.kafka

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.api.scala._

/**
  * @Author: king
  * @Datetime: 2018/10/16
  * @Desc: TODO
  *
  */
object Kafka010Example {
  def main(args: Array[String]): Unit = {

    // 解析输入参数
    val params = ParameterTool.fromArgs(args)

    if (params.getNumberOfParameters < 4) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
        + "--bootstrap.servers <kafka brokers> "
        + "--zookeeper.connect <zk quorum> --group.id <some id> [--prefix <prefix>]")
      return
    }

    val prefix = params.get("prefix", "PREFIX:")


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    // 每隔5秒创建一个检查点
    env.enableCheckpointing(5000)
    // 在Web界面中提供参数
    env.getConfig.setGlobalJobParameters(params)

    // 为卡夫卡0.10 x创建一个卡夫卡流源用户
    val kafkaConsumer = new FlinkKafkaConsumer010(
      params.getRequired("input-topic"),
      new SimpleStringSchema,
      params.getProperties)
    //消费kafka数据
    /*val transaction = env
      .addSource(
        new FlinkKafkaConsumer010[String](
          params.getRequired("input-topic"),
          new SimpleStringSchema,
          params.getProperties))
    transaction.print()*/

    //消费kafka数据
    val messageStream = env
      .addSource(kafkaConsumer)
      .map(in => prefix + in)
    messageStream.print()
    // 为卡夫卡0.10 X创建一个生产者
    val kafkaProducer = new FlinkKafkaProducer010(
      params.getRequired("output-topic"),
      new SimpleStringSchema,
      params.getProperties)

    // 将数据写入kafka
    messageStream.addSink(kafkaProducer)

    env.execute("Kafka 0.10 Example")
  }

}
