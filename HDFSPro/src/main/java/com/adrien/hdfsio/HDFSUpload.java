package com.adrien.hdfsio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSUpload {
    public static void main(String[] args) {
        Configuration entries = new Configuration();
        entries.set("dfs.replication","2");
        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"),entries,"daniel");
            //创建输入流
            FileInputStream fileInputStream = new FileInputStream(new File("/Users/luohaotian/Downloads/jd-gui-osx-1.6.6.tar"));
            //创建输出流
            FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/1221/daniel/jd-gui-osx-1.6.6.tar"));
            //对流拷
            IOUtils.copyBytes(fileInputStream,fsDataOutputStream,entries);
            //关闭资源
            IOUtils.closeStream(fsDataOutputStream);
            IOUtils.closeStream(fileInputStream);
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
