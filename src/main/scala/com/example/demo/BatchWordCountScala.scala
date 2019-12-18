package com.example.demo

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.streaming.api.scala._

object BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)
    var input: String = "E:\\flink-data\\word.txt"

    var output: String = "E:\\flink-data\\result.xls"

    println(input)
    val text = env.readTextFile(input)
    var count = text.flatMap(_.toLowerCase.split("\\s")).filter(_.nonEmpty)
      .map(w => WordWithCount(w,1)).groupBy(0).sum(1)
    count.writeAsCsv(output, "\n", " ", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
    env.execute("batch word count !")
  }
  case class WordWithCount(word:String,count:Long)
}
