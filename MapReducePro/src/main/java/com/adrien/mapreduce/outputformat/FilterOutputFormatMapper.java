package com.adrien.mapreduce.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * http://www.daniel.com
 * http://www.google.com
 * http://cn.bing.com
 * http://www.daniel.com
 * http://www.sohu.com
 * http://www.sina.com
 * http://www.sin2a.com
 *
 *
 */
public class FilterOutputFormatMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value,NullWritable.get());
    }

}
