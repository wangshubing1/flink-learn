package com.king.learn.Flink.streaming.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Author: king
 * @Date: 2019-01-14
 * @Desc: TODO
 */


/**
 * 说明Flink流中迭代的示例。
 * < p >该程序总结随机数并计算添加数
 * 它以迭代流式方式执行以达到特定阈值。</p>
 *
 * <p>
 * 此示例显示如何使用：
 * < ul >
 * < li >流式迭代，
 * < li >缓冲区超时以增强延迟，
 * < li >定向输出。
 * </ul>
 * </p>
 */
public class IterateExample {
    private static final int BOUND = 10;

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // 检查输入参数
        final ParameterTool params = ParameterTool.fromArgs(args);

        // 为整数对流设置输入

        // 获取执行环境并将setBufferTimeout设置为1以启用
        // 连续刷新输出缓冲区（最低延迟）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setBufferTimeout(1);

        // 在Web界面中提供参数
        env.getConfig().setGlobalJobParameters(params);

        // 我们先通过source函数创建初始的流对象inputStream
        DataStream<Tuple2<Integer, Integer>> inputStream;
        if (params.has("input")) {
            inputStream = env.readTextFile(params.get("input")).map(new FibonacciInputMap());
        } else {
            System.out.println("Executing Iterate example with default input data set.");
            System.out.println("Use --input to specify file input.");
            inputStream = env.addSource(new RandomFibonacciSource());
        }

        // 使用5秒超时从输入创建迭代数据流
        //为了对新计算的斐波那契数列中的值以及累加的迭代次数进行存储，我们须要将二元组数据流转换为五元组数据流，并据此创建迭代对象
        IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> it = inputStream.map(new InputMap())
                .iterate(5000);

        // 应用step函数获取下一个Fibonacci数
        // 递增计数器并使用输出选择器分割输出
        SplitStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> step = it.map(new Step())
                .split(new MySelector());

        // 通过选择定向到的元组来关闭迭代
        // '迭代'输出选择器中的通道
        it.closeWith(step.select("iterate"));

        // 生成最终输出选择指向的元组
        //  'output'通道然后获得具有最大迭代计数器的输入对
        // 在1秒的滑动窗口上
        DataStream<Tuple2<Tuple2<Integer, Integer>, Integer>> numbers = step.select("output")
                .map(new OutputMap());

        // 发出结果
        if (params.has("output")) {
            numbers.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            numbers.print();
        }

        // execute the program
        env.execute("Streaming Iteration Example");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * 生成从1到2范围内的随机整数对。
     * 该source函数会生成二元组序列，二元组的两个字段值是随机生成的作为斐波那契数列的初始值
     */
    private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        private Random rnd = new Random();

        private volatile boolean isRunning = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {

            while (isRunning && counter < BOUND) {
                int first = rnd.nextInt(BOUND / 2 - 1) + 1;
                int second = rnd.nextInt(BOUND / 2 - 1) + 1;

                ctx.collect(new Tuple2<>(first, second));
                counter++;
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * 生成0到BOUND / 2范围内的随机整数对。
     */
    private static class FibonacciInputMap implements MapFunction<String, Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Integer, Integer> map(String value) {
            String record = value.substring(1, value.length() - 1);
            String[] splitted = record.split(",");
            return new Tuple2<>(Integer.parseInt(splitted[0]), Integer.parseInt(splitted[1]));
        }
    }

    /**
     * 元组转换的map函数实现
     * 映射输入，以便在保留原始输入元组的同时计算下一个斐波纳契数。
     * 计数器附加到元组，并在每个迭代步骤中递增。
     */
    public static class InputMap implements MapFunction<Tuple2<Integer, Integer>, Tuple5<Integer, Integer, Integer,
            Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) {
            return new Tuple5<>(value.f0, value.f1, value.f0, value.f1, 0);
        }
    }

    /**
     * 迭代步骤函数，计算下一个斐波纳契数。
     */
    public static class Step implements
            MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer,
                    Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer,
                Integer> value) {
            return new Tuple5<>(value.f0, value.f1, value.f3, value.f2 + value.f3, ++value.f4);
        }
    }

    /**
     * OutputSelector测试哪个元组需要再次迭代。
     */
    public static class MySelector implements OutputSelector<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Iterable<String> select(Tuple5<Integer, Integer, Integer, Integer, Integer> value) {
            List<String> output = new ArrayList<>();
            if (value.f2 < BOUND && value.f3 < BOUND) {
                output.add("iterate");
            } else {
                output.add("output");
            }
            return output;
        }
    }

    /**
     * 退回输入对和计数器。
     */
    public static class OutputMap implements MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,
            Tuple2<Tuple2<Integer, Integer>, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Tuple2<Integer, Integer>, Integer>
        map(Tuple5<Integer, Integer, Integer, Integer, Integer>value) {
            return new Tuple2<>(new Tuple2<>(value.f0, value.f1), value.f4);
        }
    }
}
