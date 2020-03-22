package com.kerlier.scala.socket

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 开发步骤：
  * 1. 获取执行环境
  * 2. 加载/初始化数据
  * 3. 指定算子(操作数据)
  * 4. 指定数据存放的位置
  * 5. 通过execute()执行程序
  *
  * 使用nc -l -p 9000端口发送数据(需要安装netcat)
  */
object FlinkStreamDemo {
  def main(args: Array[String]): Unit = {

     var port= 9000

     //先获取一个执行的环境
     val environment = StreamExecutionEnvironment.getExecutionEnvironment

     //socketTextStream读取端口的信息
     val text = environment.socketTextStream("localhost",9000,'\n')

     import org.apache.flink.api.scala._
     val wordCounts = text.flatMap(line=>line.split("\\s"))
       .map(w=> WordCount(w,1))
       .keyBy(0)
       .timeWindow(Time.seconds(2),Time.seconds(1))
       .sum("count");//前面是时间区间，后面是时间间隔，就是每隔1s对2s的数据进行计算

     wordCounts.print().setParallelism(1)

     environment.execute("pord word count")

  }



  case class WordCount(word:String, count:Long)

}
