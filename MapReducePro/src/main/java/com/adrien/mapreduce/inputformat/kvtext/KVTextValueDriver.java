package com.adrien.mapreduce.inputformat.kvtext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 	统计输入文件中每一行的第一个单词相同的行数。
 */
public class KVTextValueDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/luohaotian/Downloads/Jennifer/HelloApp/input/keyValue",
                "/Users/luohaotian/Downloads/Jennifer/HelloApp/output/keyValue"};
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,"-");
        // 1 获取job对象
        Job job = Job.getInstance(conf);
        // 2 设置jar存储路径
        job.setJarByClass(KVTextValueDriver.class);
        // 3 关联mapper和reducer类
        job.setMapperClass(KVTextValueMapper.class);
        job.setReducerClass(KVTextValueReducer.class);
        // 4 设置mapper输出的key和value类型
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        // 6 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 1:0);
    }
}
