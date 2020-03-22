package com.kerlier.scala.datastream.datasource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object DataStreamSourceApp {

  def main(args: Array[String]): Unit = {

    //获取执行环境 , 执行环境使用val来修饰
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

//    parallelSourceFunction(environment)

    richParallelSourceFunction(environment)

    environment.execute("DataStreamSourceApp")
  }

  def noParallelSourceFunction(env: StreamExecutionEnvironment):Unit = {
    import org.apache.flink.api.scala._
    val data = env.addSource(new FashionNoParallelSource())
    data.print()
  }

  def parallelSourceFunction(env: StreamExecutionEnvironment):Unit= {

    import org.apache.flink.api.scala._
    // 设置三个并行度，相当于有三个线程在执行
    val data = env.addSource(new FashionParallelSource()).setParallelism(3)
    data.print()
  }

  def richParallelSourceFunction(env: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    val data = env.addSource(new FashionRichParallelSource()).setParallelism(3)

    data.print()
  }
}
