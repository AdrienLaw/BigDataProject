package com.adrien.mapreduce.combiner2;

import com.adrien.mapreduce.simple.SimpleWordcountMapper;
import com.adrien.mapreduce.simple.SimpleWordcountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountCombiner2Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/luohaotian/Downloads/Jennifer/HelloApp/input/combiner",
                "/Users/luohaotian/Downloads/Jennifer/HelloApp/output/wordcount/combiner2"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(WordCountCombiner2Driver.class);
        job.setMapperClass(SimpleWordcountMapper.class);
        job.setReducerClass(SimpleWordcountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(SimpleWordcountReducer.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7. 提交Job
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);
    }
}
