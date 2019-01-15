package com.king.learn.Flink.streaming.join.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import static com.king.learn.Flink.streaming.join.JoinUtil.test;

/**
 * @Author: king
 * @Date: 2019-01-14
 * @Desc: TODO 数据源
 */

public class StreamDataSource extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
    private volatile boolean running = true;
    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        Tuple3[] elements = new Tuple3[]{
                Tuple3.of("a", "1", 1000000050000L),
                Tuple3.of("a", "2", 1000000054000L),
                Tuple3.of("a", "3", 1000000079900L),
                Tuple3.of("a", "4", 1000000115000L),
                Tuple3.of("b", "5", 1000000100000L),
                Tuple3.of("b", "6", 1000000108000L)
        };

        test(ctx, elements, running);
        }


    @Override
    public void cancel() {
        running = false;

    }
}
