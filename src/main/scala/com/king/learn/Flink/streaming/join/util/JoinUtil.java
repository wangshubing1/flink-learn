package com.king.learn.Flink.streaming.join.util;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author: king
 * @Date: 2019-01-14
 * @Desc: TODO
 */

public class JoinUtil {
    /*
    flink驱动注册
     */
    public static StreamExecutionEnvironment getEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        return env;
    }
    public static DataStream<Tuple3<String, String, Long>>
    getDataStream(DataStream<Tuple3<String, String, Long>> rightSource) {
        long delay = 5100L;
        // 设置水位线
        return rightSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                    private static final long serialVersionUID = 518406720598977074L;

                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                }
        );
    }

    public static void test(SourceFunction.SourceContext<Tuple3<String, String, Long>> ctx, Tuple3[] elements, boolean running) throws InterruptedException {
        int count = 0;
        while (running && count < elements.length) {
            ctx.collect(new Tuple3<>((String) elements[count].f0, (String) elements[count].f1, (long) elements[count].f2));
            count++;
            Thread.sleep(1000);
        }
    }
}
