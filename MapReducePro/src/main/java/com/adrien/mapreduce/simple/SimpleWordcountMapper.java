package com.adrien.mapreduce.simple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN 输入数据的 key
 * VALUEIN 输入数据的 value
 * KEYOUT 输出数据的 key 的类型
 * VALUEOUT 输出数据的 value 的类型
 *
 */
public class SimpleWordcountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text text = new Text();
    IntWritable intWritable =new IntWritable();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splitWord = line.split(" ");
        for (String word : splitWord) {
            Text outKey = new Text();
            IntWritable outValue = new IntWritable();
            outKey.set(word);
            outValue.set(1);
            context.write(outKey,outValue);
        }
    }
}
