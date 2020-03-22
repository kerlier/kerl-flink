package com.kerlier.scala.datastream.datasource

/**
  *  dataStream 中 有基本三个组件：
  *  dataSource(数据源), transformation(算子), sink(输出)
  */
object LearnDataStreamAPI {
    def main(args: Array[String]): Unit = {

      //1. 读取文件 readTextFile,文件遵循TextInputFormat逐行读取并返回
      //2. 基于socket,读取每个端口中的数据, 元素通过分隔符
      //3. fromCollection,通过Collection集合创建一个数据流，集合中的所有元素都是同类型的
      //4. addSource, 第三方输入，常见的是 kafka,MQ. flink 有内置的connector连接器

      /**
        * 5. 另外可以自定义数据源,
        *   5.1 实现SourceFunction接口自定义数据源(这个无并行度)
        *   5.2 实现ParallelSourceFunction接口自定义数据源(这个有并行度)
        */

      //example1: 模拟产生从1开始的递增数据, 每次递增1




    }
}
