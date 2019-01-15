package com.king.learn.Flink.streaming.join.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import static com.king.learn.Flink.streaming.join.util.JoinUtil.test;

/**
 * @Author: king
 * @Date: 2019-01-14
 * @Desc: TODO
 */

public class StreamDataSource1 extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
    private static final long serialVersionUID = -8338462943401114121L;
    private volatile boolean running = true;
    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        Tuple3[] elements = new Tuple3[]{
                Tuple3.of("a", "hangzhou", 1000000059000L),
                Tuple3.of("b", "beijing", 1000000105000L),
        };

        test(ctx, elements, running);
    }

    @Override
    public void cancel() {
        running = false;
    }
}
