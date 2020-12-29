package com.adrien.mapreduce.comparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ComFlowBeanDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/luohaotian/Downloads/Jennifer/HelloApp/input/sort"
                ,"/Users/luohaotian/Downloads/Jennifer/HelloApp/output/sort"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ComFlowBeanDriver.class);
        job.setMapperClass(ComFlowBeanMapper.class);
        job.setReducerClass(ComFlowBeanReducer.class);
        job.setMapOutputKeyClass(ComFlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(ComFlowBean.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0: 1);
    }
}
