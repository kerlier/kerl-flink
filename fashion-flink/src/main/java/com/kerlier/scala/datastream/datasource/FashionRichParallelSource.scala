package com.kerlier.scala.datastream.datasource

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
  * 通过继承RichParallelSourceFunction来实现多并行度的数据源
  */
class FashionRichParallelSource extends  RichParallelSourceFunction[Long]{
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
