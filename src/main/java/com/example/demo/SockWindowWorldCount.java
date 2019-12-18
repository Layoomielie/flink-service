package com.example.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.lang.reflect.Parameter;

public class SockWindowWorldCount {
    public static void main(String[] args) throws Exception {
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
             port = parameterTool.getInt("port");
        }catch (Exception e){
            port=9000;
            System.err.println("使用默认端口：9000");
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname="101.132.37.5";
        String delimiter="\n";
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        SingleOutputStreamOperator<WordCount> sum = text.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String split : splits) {
                    out.collect(new WordCount(split, 1L));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1)).sum("count");
        sum.print().setParallelism(1);
        env.execute("Sock window count");
    }
    public static class WordCount{
        public String word;
        public long count;
        public  WordCount(){}
        public WordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}

