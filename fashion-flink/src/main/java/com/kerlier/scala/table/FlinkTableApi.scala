package com.kerlier.scala.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sources.CsvTableSource

/**
  * 学习table的Api, 需要加上依赖
  */
object FlinkTableApi {

  def main(args: Array[String]): Unit = {
    //第一步： 先获取基本环境， stream或者batch
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //第二步：获取一个表环境
    val tableEnv = TableEnvironment.getTableEnvironment(environment)

    //创建csvSource的时候，需要指定header名以及类型
    val fieldNames:Array[String]= Array("name", "age")
    val fieldTypes:Array[TypeInformation[_]]= Array(Types.STRING,Types.INT)

    import org.apache.flink.api.scala._
    val csvTableSource = new CsvTableSource("E:\\git\\flink\\fashion-flink\\src\\main\\java\\com\\kerlier\\scala\\table_data\\csv_table.csv", fieldNames, fieldTypes)

    //注册一个tableSource
    tableEnv.registerTableSource("csvTableSource", csvTableSource)

    //获取scan对象
//    val csvTable = tableEnv.scan("csvTableSource")
//
//    val csvResults = csvTable.select("name,age").where("name")

    val csvResults = tableEnv.sqlQuery("select name,age from csvTableSource where name is not null and age is not null")

    val persons = tableEnv.toAppendStream[Person](csvResults)

    persons.print()
    //执行sql语句
//    tableEnv.sqlQuery("select NAME from ")

    environment.execute("csvTableSourceApp")

  }
  case class Person(name:String, age:Integer)
}
