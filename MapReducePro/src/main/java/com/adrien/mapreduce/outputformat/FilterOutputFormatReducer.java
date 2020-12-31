package com.adrien.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterOutputFormatReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    Text text = new Text();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String string = key.toString();
        String line = string + "\r\t";
        text.set(line);
        for (NullWritable value : values) {
            context.write(text,NullWritable.get());
        }
    }
}
