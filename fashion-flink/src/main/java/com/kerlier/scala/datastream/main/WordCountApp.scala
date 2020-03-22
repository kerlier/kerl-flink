package com.kerlier.scala.datastream.main

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 输入源：包含句子的一个集合
  * 实现wordCount
  */
object WordCountApp {

  def main(args: Array[String]): Unit = {

    var environment = StreamExecutionEnvironment.getExecutionEnvironment

    var dataSource = List("This is begin to learn flink I am very happy", "I am happy")

    import org.apache.flink.api.scala._
    val data = environment.fromCollection(dataSource)

    data.flatMap(x=>x.split(" ")) //先将句子根据空格切分
      .map(WordCount(_, 1))// 将wordCount进行计数
      .keyBy("word")
//      .sum("count")
      .reduce((a,b)=> WordCount(a.word,a.count + b.count ))
      .print()

    environment.execute("wordCountApp")
  }

  case class WordCount(word:String, count:Integer)
}
