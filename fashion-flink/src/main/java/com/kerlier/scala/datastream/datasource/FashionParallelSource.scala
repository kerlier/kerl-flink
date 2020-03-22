package com.kerlier.scala.datastream.datasource

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
  * 自定义有并行度的source,继承ParallelSourceFunction
  */
class FashionParallelSource extends ParallelSourceFunction[Long]{

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
