package com.kerlier.scala.datastream.api

import java.util

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * filter 支持 x=> boolean 的操作
  * java 操作是 x-> boolean ,一个是等号，一个是横线
  */
object FashionFilterApi {

  def main(args: Array[String]): Unit = {

    //自定义数据源
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    val listSource =  List(1, 2, 3, 4)
    import org.apache.flink.api.scala._
    val data = env.fromCollection(listSource)

    //filter支持x=>boolean 表示式
    data.filter(x => x>3).map(x=>FashionNumber(x, "str"+x)).print()

    //使用map操作

    env.execute("FashionFilterApp")
  }

  /**
    * case class一般用于模式匹配
    * @param number
    * @param name
    */
  case class FashionNumber(number:Integer, name:String)

}
