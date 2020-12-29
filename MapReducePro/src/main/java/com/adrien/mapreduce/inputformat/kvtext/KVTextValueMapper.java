package com.adrien.mapreduce.inputformat.kvtext;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * hive-ni flink
 * spark-hadoop
 * hive-kafka flink
 * spark-hadoop
 *
 * hive	2
 * spark	2
 */
public class KVTextValueMapper extends Mapper<Text,Text,Text, IntWritable> {
    IntWritable intWritable = new IntWritable();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key,intWritable);
    }
}
