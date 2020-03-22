package com.kerlier.scala.datastream.datasource

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * 生成一个并行度为1的dataSource
  */
class FashionNoParallelSource extends  SourceFunction[Long]{

  var count = 1l
  var isRunning = true

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
     while(isRunning){
       sourceContext.collect(count)
       count += 1
       Thread.sleep(1000)
     }
  }

  override def cancel(): Unit = {
      isRunning = false
  }
}
