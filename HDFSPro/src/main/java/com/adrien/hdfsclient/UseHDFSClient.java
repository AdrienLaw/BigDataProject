package com.adrien.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class UseHDFSClient {

    @Test
    public void copyFromLocalFile() {
        //获取HDFS对象
        Configuration entries = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), entries, "daniel");
            fileSystem.copyFromLocalFile(new Path("/Users/luohaotian/Downloads/Adrien/图片/WechatIMG603.jpeg"),
                    new Path("/1221/daniel"));
            fileSystem.close();
            System.out.println("=====SUCCESS======");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 设置优先级
     */
    @Test
    public void copyFromLocalFileSetPro() {
        Configuration entries = new Configuration();
        entries.set("dfs.replication","2");
        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), entries, "daniel");
            fileSystem.copyFromLocalFile(new Path("/Users/luohaotian/Downloads/Adrien/图片/205651-15114418115112.jpg"),
                    new Path("/1221/daniel"));
            System.out.println("=====SUCCESS======");
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

//    /**
//     * @param delSrc
//     *          whether to delete the src
//     * @param src
//     *          path
//     * @param dst
//     *          path
//     * @param useRawLocalFileSystem
//     *          whether to use RawLocalFileSystem as local file system or not.
//     */

    @Test
    public void copyToLocalFile() {
        Configuration entries = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"),
                    entries, "daniel");
            fileSystem.copyToLocalFile(false,new Path("/1221/daniel/205651-15114418115112.jpg/205651-15114418115112.jpg"),
                    new Path("/Users/luohaotian/Downloads"),true);
            fileSystem.close();
        } catch (IOException e) {
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }



    @Test
    public void deleteFile() {
        Configuration entries = new Configuration();
        entries.set("dfs.replication","2");
        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), entries, "daniel");
            fileSystem.delete(new Path("/1221/daniel/p2618523428.webp"),true);
            //fileSystem.copyFromLocalFile(new Path("/Users/luohaotian/Downloads/Adrien/图片/205651-15114418115112.jpg"),
                    //new Path("/1221/daniel"));
            fileSystem.close();
            System.out.println("=====SUCCESS======");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }


    /**
     * 判断 文件 or 文件夹
     * 遍历文件目录
     */
    @Test
    public void listStatus() {
        Configuration entries = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), entries, "daniel");
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isFile()) {
                    System.out.println(fileStatus.getPath().getName() + " is a File");
                } else {
                    System.out.println(fileStatus.getPath().getName() + " is a Directory");
                }
            }
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
    public void listFiles() {
        Configuration entries = new Configuration();
        try {
            try {
                FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), entries, "daniel");
                RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
                while (listFiles.hasNext()) {
                    LocatedFileStatus fileStatus = listFiles.next();
                    System.out.println("=分组= " + fileStatus.getGroup());
                    System.out.println("=权限= " + fileStatus.getPermission());
                    BlockLocation[] blockLocations = fileStatus.getBlockLocations();
                    for (BlockLocation blockLocation : blockLocations) {
                        String[] hosts = blockLocation.getHosts();
                        for (String host : hosts) {
                        System.out.println("=IP+ "+host);
                        }
                    }
                }
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();

        }
    }

    @Test
    public void rename() {
        Configuration entries = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), entries, "daniel");
            fileSystem.rename(new Path("/1221/daniel/205651-15114418115112.jpg"),new Path("/1221/daniel/WechatIMG604.jpg"));
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
