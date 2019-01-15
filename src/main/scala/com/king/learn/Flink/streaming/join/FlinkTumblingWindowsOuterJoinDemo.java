package com.king.learn.Flink.streaming.join;


import com.king.learn.Flink.streaming.join.bean.Element;

import com.king.learn.Flink.streaming.join.source.StreamDataSource1;
import com.king.learn.Flink.streaming.join.source.StreamDataSource2;
import com.king.learn.Flink.streaming.join.util.JoinUtil;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;

import static com.king.learn.Flink.streaming.join.util.JoinUtil.getEnv;


/**
 * @Author: king
 * @Date: 2019-01-14
 * @Desc: TODO out join 必须左右两边去重的
 */

public class FlinkTumblingWindowsOuterJoinDemo {
    public static void main(String[] args) throws Exception {
        int windowSize = 10;
        // 设置数据源
        DataStream<Tuple3<String, String, Long>> leftSource =
                getEnv().addSource(new StreamDataSource1()).name("Demo Source");
        DataStream<Tuple3<String, String, Long>> rightSource =
                getEnv().addSource(new StreamDataSource2()).name("Demo Source");
        // join 操作
        JoinUtil.getDataStream(leftSource).coGroup(JoinUtil.getDataStream(rightSource))
                .where(new LeftSelectKey()).equalTo(new RightSelectKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new OuterJoin())
                .print();


        JoinUtil.getEnv().execute("TimeWindowDemo");
    }

    public static class OuterJoin implements CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>> {
        private static final long serialVersionUID = 844632302486386586L;

        @Override
        public void coGroup(Iterable<Tuple3<String, String, Long>> leftElements, Iterable<Tuple3<String, String, Long>> rightElements, Collector<Tuple5<String, String, String, Long, Long>> out) {
            HashMap<String, Element> left = new HashMap<>();
            HashMap<String, Element> right = new HashMap<>();
            HashSet<String> set = new HashSet<>();

            for (Tuple3<String, String, Long> leftElem : leftElements) {
                set.add(leftElem.f0);
                left.put(leftElem.f0, new Element(leftElem.f1, leftElem.f2));
            }

            for (Tuple3<String, String, Long> rightElem : rightElements) {
                set.add(rightElem.f0);
                right.put(rightElem.f0, new Element(rightElem.f1, rightElem.f2));
            }

            for (String key : set) {
                Element leftElem = getHashMapByDefault(left, key, new Element("null", -1L));
                Element rightElem = getHashMapByDefault(right, key, new Element("null", -1L));

                out.collect(new Tuple5<>(key, leftElem.getName(), rightElem.getName(), leftElem.getNumber(), rightElem.getNumber()));
            }
        }

        private Element getHashMapByDefault(HashMap<String, Element> map, String key, Element defaultValue) {
            return map.get(key) == null ? defaultValue : map.get(key);
        }
    }

    public static class LeftSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        private static final long serialVersionUID = -8189893569324632208L;

        @Override
        public String getKey(Tuple3<String, String, Long> w) {
            return w.f0;
        }
    }

    public static class RightSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        private static final long serialVersionUID = 2249963842374426629L;

        @Override
        public String getKey(Tuple3<String, String, Long> w) {
            return w.f0;
        }
    }

}
