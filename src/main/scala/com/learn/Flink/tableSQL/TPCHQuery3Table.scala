package com.learn.Flink.tableSQL

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

/**
  * @Author: king
  * @Datetime: 2018/11/12 
  * @Desc: TODO
  *
  */
object TPCHQuery3Table {

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set filter date
    val date = "1995-03-12".toDate

    // get execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val lineitems = getLineitemDataSet(env)
      .toTable(tEnv, 'id, 'extdPrice, 'discount, 'shipDate)
      .filter('shipDate.toDate > date)

    val customers = getCustomerDataSet(env)
      .toTable(tEnv, 'id, 'mktSegment)
      .filter('mktSegment === "AUTOMOBILE")

    val orders = getOrdersDataSet(env)
      .toTable(tEnv, 'orderId, 'custId, 'orderDate, 'shipPrio)
      .filter('orderDate.toDate < date)

    val items =
      orders.join(customers)
        .where('custId === 'id)
        .select('orderId, 'orderDate, 'shipPrio)
        .join(lineitems)
        .where('orderId === 'id)
        .select(
          'orderId,
          'extdPrice * (1.0f.toExpr - 'discount) as 'revenue,
          'orderDate,
          'shipPrio)

    val result = items
      .groupBy('orderId, 'orderDate, 'shipPrio)
      .select('orderId, 'revenue.sum as 'revenue, 'orderDate, 'shipPrio)
      .orderBy('revenue.desc, 'orderDate.asc)

    // emit result
    result.writeAsCsv(outputPath, "\n", "|")

    // execute program
    env.execute("Scala TPCH Query 3 (Table API Expression) Example")
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Lineitem(id: Long, extdPrice: Double, discount: Double, shipDate: String)
  case class Customer(id: Long, mktSegment: String)
  case class Order(orderId: Long, custId: Long, orderDate: String, shipPrio: Long)

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var lineitemPath: String = _
  private var customerPath: String = _
  private var ordersPath: String = _
  private var outputPath: String = _

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 4) {
      lineitemPath = args(0)
      customerPath = args(1)
      ordersPath = args(2)
      outputPath = args(3)
      true
    } else {
      System.err.println("This program expects data from the TPC-H benchmark as input data.\n" +
        " Due to legal restrictions, we can not ship generated data.\n" +
        " You can find the TPC-H data generator at http://www.tpc.org/tpch/.\n" +
        " Usage: TPCHQuery3 <lineitem-csv path> <customer-csv path> " +
        "<orders-csv path> <result path>")
      false
    }
  }

  private def getLineitemDataSet(env: ExecutionEnvironment): DataSet[Lineitem] = {
    env.readCsvFile[Lineitem](
      lineitemPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 5, 6, 10) )
  }

  private def getCustomerDataSet(env: ExecutionEnvironment): DataSet[Customer] = {
    env.readCsvFile[Customer](
      customerPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 6) )
  }

  private def getOrdersDataSet(env: ExecutionEnvironment): DataSet[Order] = {
    env.readCsvFile[Order](
      ordersPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 1, 4, 7) )
  }
}
