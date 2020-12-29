package com.adrien.hdfsio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSReadFileSeek {

    @Test
    public void ReadFileSeek01() {
        Configuration entries = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), entries, "daniel");
            //一个750.77 MB的zip,一个Block设置的是256
            FSDataInputStream dataInputStream = fileSystem.open(new Path("/Jennifer.zip"));
            FileOutputStream fileOutputStream = new FileOutputStream(new File("/Users/luohaotian/Downloads/Jennifer/Jennifer.zip.part01"));
            byte[] bytes = new byte[1024];
            //对流拷
            for (int i = 0; i < 1024 * 256; i++) {
                dataInputStream.read(bytes);
                fileOutputStream.write(bytes);
            }
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

    @Test
    public void ReadFileSeek02() {
        Configuration entries = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), entries, "daniel");
            //一个750.77 MB的zip,一个Block设置的是256
            FSDataInputStream dataInputStream = fileSystem.open(new Path("/Jennifer.zip"));
            FileOutputStream fileOutputStream = new FileOutputStream(new File("/Users/luohaotian/Downloads/Jennifer/Jennifer.zip.part02"));
            byte[] bytes = new byte[1024];
            //对流拷
            for (int i = 1024 * 256; i < 1024 * 512; i++) {
                dataInputStream.read(bytes);
                fileOutputStream.write(bytes);
            }
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

    @Test
    public void ReadFileSeek03() {
        Configuration entries = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), entries, "daniel");
            //一个750.77 MB的zip,一个Block设置的是256
            FSDataInputStream dataInputStream = fileSystem.open(new Path("/Jennifer.zip"));
            FileOutputStream fileOutputStream = new FileOutputStream(new File("/Users/luohaotian/Downloads/Jennifer/Jennifer.zip.part03"));
            //byte[] bytes = new byte[1024];
            dataInputStream.seek(1024 * 1024 * 512);
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