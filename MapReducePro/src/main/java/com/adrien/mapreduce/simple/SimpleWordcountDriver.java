package com.adrien.mapreduce.simple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SimpleWordcountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/luohaotian/Downloads/Jennifer/HelloApp/input/wordCount",
                "/Users/luohaotian/Downloads/Jennifer/HelloApp/output/wordCount"};
        Configuration configuration = new Configuration();
        //1. 获取 Job 对象
        Job job = Job.getInstance(configuration);
        //2. 设置 jar 的路径
        job.setJarByClass(SimpleWordcountDriver.class);
        //3.关联 mapper 和 reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //4. 设置 mapper 输出的 key 和 value 类型
        job.setMapperClass(SimpleWordcountMapper.class);
        job.setReducerClass(SimpleWordcountReduce.class);
        //5. 设置最终输出的 key 和 value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 1:0);
    }
}
