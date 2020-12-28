package com.adrien.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class UseHDFSmkDirPro {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration entries = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), entries, "daniel");
        fileSystem.mkdirs(new Path("/1221/daniel"));
        fileSystem.close();
    }
}
