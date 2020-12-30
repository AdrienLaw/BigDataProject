package com.adrien.mapreduce.groupcomparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderGroupComparatorDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/luohaotian/Downloads/Jennifer/HelloApp/input/groupComparator",
                "/Users/luohaotian/Downloads/Jennifer/HelloApp/output/groupComparator"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(OrderGroupComparatorDriver.class);
        job.setMapperClass(OrderGroupComparatorMapper.class);
        job.setReducerClass(OrderGroupComparatorReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        // 6 设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 8 设置reduce端的分组
        job.setGroupingComparatorClass(OrderGroupingComparator.class);
        // 7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
