package com.adrien.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestZookeeper {
    private String connectString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private int sessionTimeout = 2000;
    ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                List<String> children = null;
                try {
                    children = zkClient.getChildren("/", true);
                    for (String child : children) {
                        System.out.println(child);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                for (String child : children) {

                }
            }
        });
    }

    //创建子节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String path = zkClient.create("/daniel", "doge".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("==PATH== "+path);
    }

    //获取子节点并监听节点变化
    @Test
    public void getChilderDataAndWatch() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    //判断Znode是否存在判断Znode是否存在
    @Test
    public void isExists() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/adrien", true);
        System.out.println(exists == null ? 0 : 1);
    }
}
