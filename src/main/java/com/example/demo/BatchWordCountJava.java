package com.example.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 * @author：张鸿建
 * @time：2019/12/18 13:12
 * @desc：
 **/
public class BatchWordCountJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String input = "E:\\flink-data\\word_java.txt";
        String output = "E:\\flink-data\\result_java.xls";
        DataSource<String> textFile = env.readTextFile(input);
        AggregateOperator<Tuple2<String, Integer>> counts = textFile.flatMap(new Tokenizer()).groupBy(0).sum(1);
        counts.writeAsCsv(output, "\n", " ", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute("batch java word count");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\s");
            for (String token : tokens) {
                out.collect(new Tuple2<>(token, 1));
            }
        }
    }
}
