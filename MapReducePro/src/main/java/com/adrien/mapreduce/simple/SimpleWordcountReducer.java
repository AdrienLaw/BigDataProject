package com.adrien.mapreduce.simple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SimpleWordcountReducer extends Reducer<Text, IntWritable, Text,IntWritable> {

    IntWritable writable = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        writable.set(sum);
        context.write(key,writable);
    }
}
