package com.king.learn.Flink.streaming.join;


import com.king.learn.Flink.streaming.join.source.StreamDataSource;
import com.king.learn.Flink.streaming.join.source.StreamDataSource1;
import com.king.learn.Flink.streaming.join.util.JoinUtil;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.common.functions.JoinFunction;

import static com.king.learn.Flink.streaming.join.util.JoinUtil.getEnv;

/**
 * @Author: king
 * @Date: 2019-01-14
 * @Desc: TODO Inner Join
 */

public class FlinkTumblingWindowsInnerJoinDemo {
    public static void main(String[] args) throws Exception {
        int windowSize = 10;
        // 设置数据源
        DataStream<Tuple3<String, String, Long>> leftSource =
                getEnv().addSource(new StreamDataSource()).name("Demo Source");
        DataStream<Tuple3<String, String, Long>> rightSource =
                getEnv().addSource(new StreamDataSource1()).name("Demo Source");
        // join 操作
        JoinUtil.getDataStream(leftSource).join(JoinUtil.getDataStream(rightSource))
                .where(new LeftSelectKey())
                .equalTo(new RightSelectKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply((JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>,
                        Tuple5<String, String, String, Long, Long>>)
                        (first, second) -> new Tuple5<>(first.f0, first.f1, second.f1, first.f2, second.f2)).print();

        getEnv().execute("TimeWindowDemo");
    }


    public static class LeftSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        private static final long serialVersionUID = 3962206049185587477L;

        @Override
        public String getKey(Tuple3<String, String, Long> w) {
            return w.f0;
        }
    }

    public static class RightSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        private static final long serialVersionUID = -5385125386985167962L;

        @Override
        public String getKey(Tuple3<String, String, Long> w) {
            return w.f0;
        }
    }

}
