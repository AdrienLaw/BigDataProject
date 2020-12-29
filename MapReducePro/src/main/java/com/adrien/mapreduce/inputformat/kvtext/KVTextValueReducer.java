package com.adrien.mapreduce.inputformat.kvtext;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * hive-ni flink
 * spark-hadoop
 * hive-kafka flink
 * spark-hadoop
 *
 *
 * hive-ni	2
 * hive-kafka	2
 */
public class KVTextValueReducer extends Reducer<Text, IntWritable,Text, Writable> {
    IntWritable intWritable = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum =  0;
        for (IntWritable value : values) {
            sum ++;
        }
        intWritable.set(sum);
        context.write(key,intWritable);

    }
}
