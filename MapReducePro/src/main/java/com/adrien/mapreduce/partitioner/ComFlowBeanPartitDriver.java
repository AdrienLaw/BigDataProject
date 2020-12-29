package com.adrien.mapreduce.partitioner;

import com.adrien.mapreduce.comparable.ComFlowBean;
import com.adrien.mapreduce.comparable.ComFlowBeanDriver;
import com.adrien.mapreduce.comparable.ComFlowBeanMapper;
import com.adrien.mapreduce.comparable.ComFlowBeanReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ComFlowBeanPartitDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/luohaotian/Downloads/Jennifer/HelloApp/input/sort",
                "/Users/luohaotian/Downloads/Jennifer/HelloApp/output/sort/partition"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(ComFlowBeanPartitDriver.class);
        job.setMapperClass(ComFlowBeanMapper.class);
        job.setReducerClass(ComFlowBeanReducer.class);
        job.setMapOutputKeyClass(ComFlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ComFlowBean.class);
        job.setPartitionerClass(SubThreePartitioner.class);
        job.setNumReduceTasks(5);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);
    }
}
