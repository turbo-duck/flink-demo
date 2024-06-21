package icu.wzk.demo01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;


public class StartApp {

    public static void main(String[] args) throws Exception {
        String inPath = "demo01/file.txt";
        String outputPath = "demo01/result.csv";
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = executionEnvironment.readTextFile(inPath);
        DataSet<Tuple2<String, Integer>> dataSet = text
                .flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);
        dataSet
                .writeAsCsv(outputPath,"\n"," ", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
        executionEnvironment.execute("file.txt -> result.csv");
    }

    static class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : line.split(" ")) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
