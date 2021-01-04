package com.adrien.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class zkClient {
    private String connectString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private int sessionTime = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";

    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTime, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getServers();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //2. 监听节点变化
    public void getServers() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(parentNode, true);
        ArrayList<String> servers = new ArrayList<>();
        for (String child : children) {
            byte[] data = zk.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }
        System.out.println(servers);
    }

    //业务逻辑
    public void business() throws InterruptedException {
        System.out.println("=====进行客户端业务处理中....=====");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        zkClient zkClient = new zkClient();
        zkClient.getConnect();
        zkClient.getServers();
        zkClient.business();
    }
}
