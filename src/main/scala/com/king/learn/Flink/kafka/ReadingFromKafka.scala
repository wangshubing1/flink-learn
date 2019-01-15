package com.king.learn.Flink.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._

/**
  * @Author: king
  * @Datetime: 2018/11/26 
  * @Desc: TODO
  *
  */
object ReadingFromKafka {
  private val ZOOKEEPER_HOST = "master:2181,worker1:2181,worker2:2181"
  private val KAFKA_BROKER = "master:9092,worker1:9092,worker2:9092"
  private val TRANSACTION_GROUP = "transaction"

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    //topicd的名字是new，schema默认使用SimpleStringSchema()即可
    val transaction = env
      .addSource(
        new FlinkKafkaConsumer010[String]("new", new SimpleStringSchema, kafkaProps)
      )

    transaction.print()

    env.execute()

  }

}
