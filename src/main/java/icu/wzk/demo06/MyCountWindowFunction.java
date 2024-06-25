package icu.wzk.demo06;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;


/**
 * 基于事件驱动 GlobalWindow
 * @author wzk
 * @date 10:27 2024/6/22
**/
public class MyCountWindowFunction implements WindowFunction<Tuple2<String, Integer>, String, String, GlobalWindow> {

    @Override
    public void apply(String s, GlobalWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        int sum = 0;
        for (Tuple2<String, Integer> tuple2 : input){
            sum += tuple2.f1;
        }
        // 无用的时间戳，默认值为： Long.MAX_VALUE,因为基于事件计数的情况下，不关心时间。
        long maxTimestamp = window.maxTimestamp();
        out.collect("key:" + s + " value: " + sum + "| maxTimeStamp :"
                + maxTimestamp + "," + format.format(maxTimestamp)
        );
    }
}
