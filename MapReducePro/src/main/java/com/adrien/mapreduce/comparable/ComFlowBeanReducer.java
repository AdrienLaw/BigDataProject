package com.adrien.mapreduce.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ComFlowBeanReducer extends Reducer<ComFlowBean, Text,Text,ComFlowBean> {
    @Override
    protected void reduce(ComFlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
