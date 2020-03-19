package com.atguigu.java.wc;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Project:mylearn
 * Package:com.atguigu.java
 * Version:1.0
 * Created by yinzeyang on 2020/03/18 16:25
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\Git\\mylearn\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        // 加载或创建源数据
        //DataSet<String> text = env.fromElements("this a book", "i love china", "i am chinese");
        DataSource<String> text = env.readTextFile(path);

        // 转化处理数据
        DataSet<Tuple2<String, Integer>> ds = text.flatMap(new LineSplitter()).groupBy(0).sum(1);

        DataSet<Tuple2<String,Integer>> ds2 = text.flatMap(new LineSplitter()).groupBy(0).sum(1);

        // 输出数据到目的端
        ds.print();
    }

    static class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word:line.split(" ")) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
