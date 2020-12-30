package com.adrien.mapreduce.groupcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderGroupComparatorMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    OrderBean orderBean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String stringLine = value.toString();
        String[] splits = stringLine.split("\t");
        orderBean.setOrderId(Integer.parseInt(splits[0]));
        orderBean.setPrice(Double.parseDouble(splits[2]));
        context.write(orderBean,NullWritable.get());
    }
}
