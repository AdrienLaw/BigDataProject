package com.adrien.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class UseHDFSmkdir {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        //获取客户端对象
        conf.set("fs.defaultFS", "hdfs://hadoop101:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.mkdirs(new Path("/1221/laozhou"));
        fileSystem.close();
    }
}
