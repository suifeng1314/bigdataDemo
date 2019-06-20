package com.longtao.zookeeper.watch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

public class Watch {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        // 节点
        String connected = "192.168.44.133:2181,192.168.44.134:2181,192.168.44.135:2181";
        // 超时
        int timeout = 2000;

        // 1.连接zk集群
        ZooKeeper zkCli = new ZooKeeper(connected, timeout, new Watcher() {
            // 回调方法 显示/节点
            public void process(WatchedEvent watchedEvent) {
                System.out.println("正在监听中......");
            }
        });

        // 2.监听 ls / watch get / watch
        zkCli.getChildren("/", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("监听的路径：" + watchedEvent.getPath());
                System.out.println("监听的类型：" + watchedEvent.getType());
                System.out.println("正在操作。。。。");
                // 查看子节点
//                for (String str: children) {
//                    System.out.println(str);
//                }
            }
        },null);

        Thread.sleep(Long.MAX_VALUE);

    }
}
