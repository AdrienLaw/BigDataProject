package com.adrien.hdfsio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSDownload {
    public static void main(String[] args) {
        Configuration entries = new Configuration();
        //entries.set("dfs.replication","2");
        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), entries, "daniel");
            //创建输入流
            FSDataInputStream dataInputStream = fileSystem.open(new Path("/1221/daniel/jd-gui-osx-1.6.6.tar"));
            //创建输出流
            FileOutputStream fileOutputStream = new FileOutputStream(new File("/Users/luohaotian/Downloads/Jennifer/jd-gui-balabalabala.tar"));
            //对流拷
            IOUtils.copyBytes(dataInputStream,fileOutputStream,entries);
            IOUtils.closeStream(fileOutputStream);
            IOUtils.closeStream(dataInputStream);
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}


