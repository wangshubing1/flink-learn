package com.king.learn.Flink.demo;/*
package com.learn.Flink.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

*/
/**
 * @Author: king
 * @Datetime: 2018/11/26
 * @Desc: TODO
 *//*

public class MongodbExample {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create a MongodbInputFormat, using a Hadoop input format wrapper
        HadoopInputFormat<BSONWritable, BSONWritable> hdIf = new HadoopInputFormat<BSONWritable, BSONWritable>(
                new MongoInputFormat(), BSONWritable.class, BSONWritable.class,	new JobConf());

        // specify connection parameters
        hdIf.getJobConf().set("mongo.input.uri", "mongodb://localhost:27017/dbname.collectioname");

        DataSet<Tuple2<BSONWritable, BSONWritable>> input = env.createInput(hdIf);
        // a little example how to use the data in a mapper.
        DataSet<Tuple2< Text, BSONWritable>> fin = input.map(
                record ->{
                    BSONWritable value = record.getField(1);
                    BSONObject doc = value.getDoc();
                    BasicDBObject jsonld = (BasicDBObject) doc.get("jsonld");

                    String id = jsonld.getString("@id");
                    DBObject builder = BasicDBObjectBuilder.start()
                            .add("id", id)
                            .add("type", jsonld.getString("@type"))
                            .get();

                    BSONWritable w = new BSONWritable(builder);
                    return new Tuple2<Text,BSONWritable>(new Text(id), w);
                });

        // emit result (this works only locally)
//		fin.print();

        MongoConfigUtil.setOutputURI( hdIf.getJobConf(), "mongodb://localhost:27017/test.testData");
        // emit result (this works only locally)
        fin.output(new HadoopOutputFormat<Text,BSONWritable>(new MongoOutputFormat<Text,BSONWritable>(), hdIf.getJobConf()));

        // execute program
        env.execute("Mongodb Example");
    }
}
*/
