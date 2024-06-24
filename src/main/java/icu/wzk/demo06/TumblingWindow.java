package icu.wzk.demo06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.api.java.tuple.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Random;


/**
 * 滚动时间窗口 Tumbling Window
 * 时间对齐，窗口长度固定，没有重叠
 * @author wzk
 * @date 10:48 2024/6/22
**/
public class TumblingWindow {

    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {
        //设置执行环境，类似spark中初始化sparkContext
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("0.0.0.0", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                long timeMillis = System.currentTimeMillis();
                int random = RANDOM.nextInt(10);
                System.out.println("value: " + value + " random: " + random + "timestamp: " + timeMillis + "|" + format.format(timeMillis));
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

        // =============== 时间驱动 ============================
        // 每隔10s划分一个窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> timeWindow = keyedStream
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        timeWindow.apply(new MyTimeWindowFunction()).print();

        // ================ 事件驱动 ============================
        // 每相隔3个事件(即三个相同key的数据), 划分一个窗口进行计算
        // WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> countWindow = keyedStream.countWindow(3);
        // countWindow.apply(new MyCountWindowFunction()).print();

        env.execute();
    }

}
