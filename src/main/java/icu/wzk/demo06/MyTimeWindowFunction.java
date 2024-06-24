package icu.wzk.demo06;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;


/**
 * 基于时间驱动 TimeWindow
 * @author wzk
 * @date 10:26 2024/6/22
**/
public class MyTimeWindowFunction implements WindowFunction<Tuple2<String,Integer>, String, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        int sum = 0;

        for(Tuple2<String,Integer> tuple2 : input){
            sum +=tuple2.f1;
        }

        long start = window.getStart();
        long end = window.getEnd();

        out.collect("key:" + s + " value: " + sum + "| window_start :"
                + format.format(start) + "  window_end :" + format.format(end)
        );
    }
}
