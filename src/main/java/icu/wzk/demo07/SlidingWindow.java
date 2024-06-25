package icu.wzk.demo07;

import icu.wzk.demo06.MyCountWindowFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * 滑动窗口 SlidingWindow
 * 窗口长度固定 可以有重叠
 * 基于时间驱动、基于事件驱动
 * @author wzk
 * @date 10:51 2024/6/22
**/
public class SlidingWindow {

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

        // ==================== 时间驱动 ============================
        // 基于时间驱动，每隔5s计算一下最近10s的数据
        // WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow = keyedStream.timeWindow(Time.seconds(10), Time.seconds(5));
        // timeWindow.sum(1).print();
        // timeWindow.apply(new MyTimeWindowFunction()).print();

        // =================== 事件驱动 =============================
        //基于事件驱动，每隔2个事件，触发一次计算，本次窗口的大小为3，代表窗口里的每种事件最多为3个
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> countWindow = keyedStream
                .countWindow(3, 2);
        countWindow.sum(1).print();
        countWindow.apply(new MyCountWindowFunction()).print();
        env.execute();
    }

}
