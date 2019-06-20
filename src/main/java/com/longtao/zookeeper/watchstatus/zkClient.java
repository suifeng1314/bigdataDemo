package com.longtao.zookeeper.watchstatus;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 实现所有节点监听
 */
public class zkClient {


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 1.连接集群
        zkClient client = new zkClient();
        client.getConnect();
        // 2.指定监听的路径
        client.getServers();
        // 3.业务逻辑，一直监听
        client.getWatch();

    }

    public void getWatch() throws InterruptedException {
        // 循环监听
        Thread.sleep(Long.MAX_VALUE);
    }

    public void getServers() throws KeeperException, InterruptedException {
        List<String> children = zkCli.getChildren("/",true);
        // 存储服务器列表
        List<String> serverList = new ArrayList<String>();

        for (String str : children){
            byte[] data = zkCli.getData("/" + str, true, null);
            serverList.add(new String(data));
        }
        System.out.println("服务器列表：" + serverList);
    }
    // 节点
    private final String connected = "192.168.44.133:2181,192.168.44.134:2181,192.168.44.135:2181";
    // 超时
    private int timeout = 2000;

    ZooKeeper zkCli;
    public void getConnect() throws IOException {
        zkCli = new ZooKeeper(connected, timeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                List<String> children;

                try {
                    children = zkCli.getChildren("/",true);
                    // 服务列表
                    List<String> serverList = new ArrayList<String>();
                    for (String str : children){
                        byte[] data = zkCli.getData("/" + str, true, null);
                        serverList.add(new String(data));
                    }
                    System.out.println("服务器列表：" + serverList);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
