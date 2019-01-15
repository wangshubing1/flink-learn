package com.king.learn.Flink.streaming.iteration;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collections;
import java.util.Random;

/**
 * @Author: king
 * @Date: 2019-01-14
 * @Desc: TODO
 */

public class IterateTest {

    private static final int MAX_RANDOM_VALUE = 10;
    private static final int OVERFLOW_THRESHOLD = 10;
    private static final String ITERATE_FLAG = "input";
    private static final String OUTPUT_FLAG = "output";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment().setBufferTimeout(1);
        //首先，我们先通过source函数创建初始的流对象inputStream：
        DataStream<Tuple2<Integer, Integer>> inputStream = env.addSource(new RandomFibonacciSource());
         /*为了对新计算的斐波那契数列中的值以及累加的迭代次数进行存储，我们须要将二元组数据流转换为五元组数据流，并据此创建迭代对象：
        注意下面代码段中iterate API的參数5000，不是指迭代5000次，而是等待反馈输入的最大时间间隔为5秒。
        流被觉得是无界的。所以无法像批处理迭代那样指定最大迭代次数。但它同意指定一个最大等待间隔，
        假设在给定的时间间隔里没有元素到来。那么将会终止迭代。
         */
        IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> iterativeStream =
                inputStream.map(new TupleTransformMapFunction()).iterate(5000);
        //在迭代流iterativeStream创建完毕之后，我们将基于它运行斐波那契数列的步函数并产生斐波那契数列流fibonacciStream：
        DataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> fibonacciStream =
                iterativeStream.map(new FibonacciCalcStepFunction());
        /*我们对每一个元素计算斐波那契数列的新值并产生了fibonacciStream，
       可是我们须要对最新的两个值进行推断。看它们是否超过了指定的阈值。
       超过了阈值的元组将会被输出，而没有超过的则会再次參与迭代。
       因此这将产生两个不同的分支。我们也为此构建了分支流：
       */
        SplitStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> branchedStream =
                fibonacciStream.split(new FibonacciOverflowSelector());

        iterativeStream.closeWith(branchedStream.select(ITERATE_FLAG));

        DataStream<Tuple3<Integer, Integer, Integer>> outputStream = branchedStream
                .select(OUTPUT_FLAG).map(new BuildOutputTupleMapFunction());
        outputStream.print();
        env.execute("Streaming Iteration Example");
    }

    //该source函数会生成二元组序列，二元组的两个字段值是随机生成的作为斐波那契数列的初始值：
    private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = 86685216605524346L;
        private Random random = new Random();
        private volatile boolean isRunning = true;
        private int counter = 0;

        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while (isRunning && counter < MAX_RANDOM_VALUE) {
                int first = random.nextInt(MAX_RANDOM_VALUE / 2 - 1) + 1;
                int second = random.nextInt(MAX_RANDOM_VALUE / 2 - 1) + 1;

                if (first > second) continue;

                ctx.collect(new Tuple2<>(first, second));
                counter++;
                Thread.sleep(50);
            }
        }

        public void cancel() {
            isRunning = false;
        }
    }

    /*元组转换的map函数实现：
    Tuple2 转Tuple5
    当中索引为0。1这两个位置的元素，始终都是最初生成的两个元素不会变化，而后三个字段都会随着迭代而变化。
     */
    private static class TupleTransformMapFunction extends RichMapFunction<Tuple2<Integer,
            Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        private static final long serialVersionUID = -6585804087902215665L;

        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(
                Tuple2<Integer, Integer> inputTuples) {
            return new Tuple5<>(
                    inputTuples.f0,
                    inputTuples.f1,
                    inputTuples.f0,
                    inputTuples.f1,
                    0);
        }
    }

    /*
    当中用于计算斐波那契数列的步函数实现例如以下：
    后三个字段会产生变化。在计算之前，数列最后一个元素会被保留。
    也就是f3相应的元素，然后通过f2元素加上f3元素会产生最新值并更新f3元素。而f4则会累加。
    随着迭代次数添加，不是整个数列都会被保留。仅仅有最初的两个元素和最新的两个元素会被保留，
    这里也不是必需保留整个数列，由于我们不须要完整的数列。我们仅仅须要对最新的两个元素进行推断就可以。
     */
    private static class FibonacciCalcStepFunction extends
            RichMapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,
                    Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        private static final long serialVersionUID = 2458865947090965310L;

        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(
                Tuple5<Integer, Integer, Integer, Integer, Integer> inputTuple) {
            return new Tuple5<>(
                    inputTuple.f0,
                    inputTuple.f1,
                    inputTuple.f3,
                    inputTuple.f2 + inputTuple.f3,
                    ++inputTuple.f4);
        }
    }

    /*
    而对是否超过阈值的元组进行推断并分离的实现例如以下：
    在筛选方法select中，我们对不同的分支以不同的常量标识符进行标识：ITERATE_FLAG（还要继续迭代）和OUTPUT_FLAG（直接输出）。
    产生了分支流之后。我们就能够从中检出不同的流分支做迭代或者输出处理。
    对须要再次迭代的，就通过迭代流的closeWith方法反馈给迭代头：
     */
    private static class FibonacciOverflowSelector implements
            OutputSelector<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        private static final long serialVersionUID = -1021403248872072944L;

        public Iterable<String> select(
                Tuple5<Integer, Integer, Integer, Integer, Integer> inputTuple) {
            if (inputTuple.f2 < OVERFLOW_THRESHOLD && inputTuple.f3 < OVERFLOW_THRESHOLD) {
                return Collections.singleton(ITERATE_FLAG);
            }

            return Collections.singleton(OUTPUT_FLAG);
        }
    }

    /*
    所谓的重构就是将之前的五元组又一次缩减为三元组，实现例如以下：
     */
    private static class BuildOutputTupleMapFunction extends
            RichMapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> {
        private static final long serialVersionUID = -8789559563954055897L;

        public Tuple3<Integer, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer> inputTuple) {
            return new Tuple3<>(
                    inputTuple.f0,
                    inputTuple.f1,
                    inputTuple.f4);
        }
    }

}
