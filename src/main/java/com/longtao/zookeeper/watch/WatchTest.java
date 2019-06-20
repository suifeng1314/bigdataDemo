package com.longtao.zookeeper.watch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class WatchTest {

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 节点
        String connected = "192.168.44.133:2181,192.168.44.134:2181,192.168.44.135:2181";
        // 超时
        int timeout = 2000;
        ZooKeeper zkCli = new ZooKeeper(connected, timeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
        byte[] data = zkCli.getData("/reba", new Watcher() {
            // 监听内容
            public void process(WatchedEvent watchedEvent) {
                System.out.println("监听的路径：" + watchedEvent.getPath());
                System.out.println("监听的类型：" + watchedEvent.getType());
                System.out.println("正在操作。。。。");
            }
        },null);

        System.out.println(new String(data));
        Thread.sleep(Long.MAX_VALUE);

    }
}
