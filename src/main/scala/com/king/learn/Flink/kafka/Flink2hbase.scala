package com.king.learn.Flink.kafka

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.flink.api.scala._

/**
  * @Author: king
  * @Date: 2019-01-14
  * @Desc: TODO Inner Join
  */
object Flink2hbase {
  val ZOOKEEPER_URL = "k8sn125:2181,k8sm126:2181,k8sm134:2181"
  val KAFKA_URL = "k8sn125:9092,k8sm126:9092,k8sm134:9092"
  val columnFamily = "info"
  val tableName: TableName = TableName.valueOf("Flink2HBase")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置启动检查点（很重要）
    env.enableCheckpointing(1000)
    // 设置为TimeCharacteristic.EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val props = new Properties()
    props.setProperty("zookeeper.connect", ZOOKEEPER_URL)
    props.setProperty("bootstrap.servers", KAFKA_URL)
    props.setProperty("group.id", "flink-kafka")
    val transction = env.addSource(
      new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema, props))
    transction.rebalance.map { value =>
      print(value)
      writeIntoHBase(value)
    }
    env.execute()


  }

  def writeIntoHBase(m: String): Unit = {
    val hbaseconf = HBaseConfiguration.create
    hbaseconf.set("hbase.zookeeper.quorum", ZOOKEEPER_URL)
    hbaseconf.set("hbase.defaults.for.version.skip", "ture")
    val connection = ConnectionFactory.createConnection(hbaseconf)
    val admin = connection.getAdmin

    if (!admin.tableExists(tableName)) admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(columnFamily)))
    val table = connection.getTable(tableName)
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val put = new Put(Bytes.toBytes(df.format(new Date())))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("test"), Bytes.toBytes(m))
    table.put(put)
    table.close()

  }

}
