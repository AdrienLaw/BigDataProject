package com.adrien.mapreduce.inputformat.selfdefined;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<Text, BytesWritable> {
    FileSplit split;
    Configuration configuration;
    Text text = new Text();
    BytesWritable bytesWritable =  new BytesWritable();
    boolean isProgress = true;

    //初始化
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    //处理业务核心
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress) {
            int length = (int) split.getLength();
            byte[] buffer = new byte[length];
            //1. 获取 fs 对象
            Path path = split.getPath();
            //2.获取输入流
            FileSystem fileSystem = path.getFileSystem(configuration);
            //3. 拷贝
            FSDataInputStream fsDataInputStream = fileSystem.open(path);
            IOUtils.readFully(fsDataInputStream,buffer,0,buffer.length);
            //4. 封装 bytesWritable
            bytesWritable.set(buffer,0,buffer.length);
            //5. 封装 text
            text.set(path.toString());
            //6. 关闭资源
            IOUtils.closeStream(fsDataInputStream);
            isProgress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return text;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
