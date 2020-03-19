package com.atguigu.wc
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Project:mylearn
  * Package:com.atguigu.wc
  * Version:1.0
  * Created by yinzeyang on 2020/03/18 16:17
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputPath = "D:\\Git\\mylearn\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDateSet = env.readTextFile(inputPath)

    val wordCountDataSet = inputDateSet.flatMap(_.split(" ")).filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()
  }

}
