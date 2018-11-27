package com.learn.Flink;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @Author: king
 * @Datetime: 2018/11/27
 * @Desc: TODO
 */
public class JavaLanbda {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Integer> input = env.fromElements(1,2,3);
       try {
            input.flatMap((Integer number,Collector<String> out)->{
                StringBuilder builder = new StringBuilder();
                for (int i =0;i<number;i++){
                    builder.append("a");
                    out.collect(builder.toString());
                }
            }).returns(Types.STRING).print();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //env.fromElements(1,2,3).map(i-> Tuple2.of(i,i)).print();
        // use the explicit ".returns(...)"
        env.fromElements(1, 2, 3)
                .map(i -> Tuple2.of(i, i))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .print();

    }
}
