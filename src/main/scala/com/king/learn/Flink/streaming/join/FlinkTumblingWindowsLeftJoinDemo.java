package com.king.learn.Flink.streaming.join;


import com.king.learn.Flink.streaming.join.source.StreamDataSource;
import com.king.learn.Flink.streaming.join.source.StreamDataSource1;
import com.king.learn.Flink.streaming.join.util.JoinUtil;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import static com.king.learn.Flink.streaming.join.util.JoinUtil.getEnv;

/**
 * @Author: king
 * @Date: 2019-01-14
 * @Desc: TODO Left Outer Join
 */

public class FlinkTumblingWindowsLeftJoinDemo {
    public static void main(String[] args) throws Exception {
        int windowSize = 10;
        // 设置数据源
        DataStream<Tuple3<String, String, Long>> leftSource =
                getEnv().addSource(new StreamDataSource()).name("Demo Source");
        DataStream<Tuple3<String, String, Long>> rightSource =
                getEnv().addSource(new StreamDataSource1()).name("Demo Source");
        // join 操作
        JoinUtil.getDataStream(leftSource).coGroup(JoinUtil.getDataStream(rightSource))
                .where(new LeftSelectKey()).equalTo(new RightSelectKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new LeftJoin())
                .print();

        JoinUtil.getEnv().execute("TimeWindowDemo");
    }


    public static class LeftJoin implements CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>> {
        private static final long serialVersionUID = 3583938761914965374L;

        @Override
        public void coGroup(Iterable<Tuple3<String, String, Long>> leftElements, Iterable<Tuple3<String, String, Long>> rightElements, Collector<Tuple5<String, String, String, Long, Long>> out) {

            for (Tuple3<String, String, Long> leftElem : leftElements) {
                boolean hadElements = false;
                for (Tuple3<String, String, Long> rightElem : rightElements) {
                    out.collect(new Tuple5<>(leftElem.f0, leftElem.f1, rightElem.f1, leftElem.f2, rightElem.f2));
                    hadElements = true;
                }
                if (!hadElements) {
                    out.collect(new Tuple5<>(leftElem.f0, leftElem.f1, "null", leftElem.f2, -1L));
                }
            }
        }
    }

    public static class LeftSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        private static final long serialVersionUID = -4996755192016797420L;

        @Override
        public String getKey(Tuple3<String, String, Long> w) {
            return w.f0;
        }
    }

    public static class RightSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        private static final long serialVersionUID = -4959317241606342598L;

        @Override
        public String getKey(Tuple3<String, String, Long> w) {
            return w.f0;
        }
    }
}
