package icu.wzk.demo07;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import icu.wzk.demo06.MyTimeWindowFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.Random;


/**
 * 会话窗口
 * @author wzk
 * @date 14:10 2024/6/24
**/
public class SessionWindow {

    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("0.0.0.0", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                long timeMillis = System.currentTimeMillis();
                int random = RANDOM.nextInt(10);
                System.err.println("value : " + value + " random : " + random + " timestamp : " + timeMillis + "|" + format.format(timeMillis));
                return new Tuple2<>(value, random);
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapStream
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });

        // 如果连续10s内，没有数据进来，则会话窗口断开。
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyedStream
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));
        window.sum(1).print();
        // window.apply(new MyTimeWindowFunction()).print();
        env.execute();
    }

}
