package com.adrien.mapreduce.inputformat.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class NLineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取 job 对象
        Configuration conf = new Configuration();
        args = new String[]{"/Users/luohaotian/Downloads/Jennifer/HelloApp/input/nline",
                "/Users/luohaotian/Downloads/Jennifer/HelloApp/output/nline"};
        Job job = Job.getInstance(conf);
        // 2 设置 jar 包位置，关联 mapper 和 reducer
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);
        job.setJarByClass(NLineDriver.class);
        // 3 设置 map 输出 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 4 设置最终输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 5 设置输入输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 7 设置每个切片 InputSplit 中划分三条记录
        NLineInputFormat.setNumLinesPerSplit(job,3);
        job.setInputFormatClass(NLineInputFormat.class);
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 1 : 0);
    }
}
