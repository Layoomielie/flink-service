package com.example.demo

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import scala.collection.mutable
object SocketWindowWordCountScala {
  def main(args: Array[String]): Unit = {

    var port: Int= try {
      ParameterTool.fromArgs(args).getInt("port")
    }catch{
      case e:Exception=>{
        System.err.println("no port ,use defalut port 9000")
      }
        9000
    }

    var env: StreamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment

    var text=env.socketTextStream("101.132.37.5",port,'\n')
    var windowCount=text.flatMap(line => line.split("\\s")).map(w=>WordWithCount(w,1)).keyBy("word")
      .timeWindow(Time.seconds(2),Time.seconds(1)).sum("count")
  //reduce((a,b)=>WordWithCount(a.word,a.count+b.count))
    windowCount.print().setParallelism(1);
    env.execute("scala word count")

  }

  case class WordWithCount(word:String,count:Long)
}
