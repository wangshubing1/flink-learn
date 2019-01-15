package com.king.learn.Flink.streaming.join.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import static com.king.learn.Flink.streaming.join.JoinUtil.test;

/**
 * @Author: king
 * @Date: 2019-01-14
 * @Desc: TODO
 */

public class StreamDataSource2 extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
    private volatile boolean running = true;

    @Override
    public void run(SourceFunction.SourceContext<Tuple3<String, String, Long>> ctx) throws InterruptedException {

        Tuple3[] elements = new Tuple3[]{
                Tuple3.of("a", "beijing", 1000000058000L),
                Tuple3.of("c", "beijing", 1000000055000L),
                Tuple3.of("d", "beijing", 1000000106000L),
        };

        test(ctx, elements, running);
    }

    @Override
    public void cancel() {
        running = false;
    }
}
