package com.king.learn.Flink.streaming.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/**
 * @Author: king
 * @Date: 2019-01-16
 * @Desc: TODO  以下示例迭代地估计数量Pi。目标是计算落入单位圆的随机点数。
 *        TODO  在每次迭代中，挑选一个随机点。如果此点位于单位圆内，我们会增加计数。
 *        TODO  然后估计Pi作为结果计数除以迭代次数乘以4。
 */

public class IteratePi {

    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // Create initialIterativeDataSet
        /*要创建BulkIteration，请调用iterate(int)迭代的DataSet方法。这将返回一个IterativeDataSet，
        可以使用常规运算符进行转换。迭代调用的单个参数指定最大迭代次数。
        要指定迭代的结束，请调用closeWith(DataSet)方法IterativeDataSet以指定应将哪个转换反馈到下一次迭代。
        closeWith(DataSet, DataSet)如果此DataSet为空，您可以选择指定终止条件，该条件评估第二个DataSet并终止迭代。
        如果未指定终止条件，则迭代将在给定的最大数量迭代后终止。
        以下示例迭代地估计数量Pi。目标是计算落入单位圆的随机点数。在每次迭代中，挑选一个随机点。如果此点位于单位圆内，我们会增加计数。
        然后估计Pi作为结果计数除以迭代次数乘以4。
        */
        //IterativeStream<Integer> iteration = input.iterate();
        IterativeDataSet<Integer> initial = env.fromElements(0).iterate(10000);

        DataSet<Integer> iteration= initial.map(
                (MapFunction<Integer, Integer>) i -> {
            double x = Math.random();
            double y = Math.random();
            return i + ((x * x + y * y < 1) ? 1 : 0);
                });


        // Iterativelytransform the IterativeDataSet
        DataSet<Integer> count = initial.closeWith(iteration);

        // execute theprogram
        count.map((MapFunction<Integer, Double>) count1 -> count1 / (double) 10000 * 4).print();
        //env.execute("IterativePi Example");
    }   

}


