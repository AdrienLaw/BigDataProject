package com.adrien.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fsDataOutputStreamDaniel;
    FSDataOutputStream fsDataOutputStreamOthers;

    public FilterRecordWriter (TaskAttemptContext job) {
        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            fsDataOutputStreamDaniel = fileSystem.create(new Path("/Users/luohaotian/Downloads/Jennifer/HelloApp/output/outputFormat/adrien.log"));
            fsDataOutputStreamOthers = fileSystem.create(new Path("/Users/luohaotian/Downloads/Jennifer/HelloApp/output/outputFormat/others.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if (key.toString().contains("daniel")) {
            fsDataOutputStreamDaniel.write(key.toString().getBytes());
        } else {
            fsDataOutputStreamOthers.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fsDataOutputStreamDaniel);
        IOUtils.closeStream(fsDataOutputStreamOthers);
    }
}
