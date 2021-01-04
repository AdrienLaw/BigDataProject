package com.adrien.zk;

import org.apache.zookeeper.*;

import java.io.IOException;

public class zkServer {
    private String connectString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private int sessionTime = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";

    //
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTime, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    public void register(String hostname) throws KeeperException, InterruptedException {
        String path = zk.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname +" is online "+path);
    }

    public void bussiness() throws InterruptedException {
        System.out.println("=====来接客======");
        Thread.sleep(Long.MAX_VALUE);
    }


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        zkServer zkServer = new zkServer();
        zkServer.getConnect();
        zkServer.register("hadoop101:2181");
        zkServer.bussiness();
    }
}

