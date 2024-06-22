package icu.wzk.demo02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StartApp {

    public static void main(String[] args) throws Exception {
        String ip = "0.0.0.0";
        int port = 9999;
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textStream = streamExecutionEnvironment.socketTextStream(ip, port, "\n");
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2SingleOutputStreamOperator = textStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] splits = s.split("\\s");
                for (String word : splits) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> word = tuple2SingleOutputStreamOperator
                .keyBy(new KeySelector<Tuple2<String, Long>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return stringLongTuple2.f0;
                    }
                })
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .sum(1);
        word.print();
        streamExecutionEnvironment.execute("stream!");
    }
}
