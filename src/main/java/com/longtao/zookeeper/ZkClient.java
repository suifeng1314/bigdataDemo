package com.longtao.zookeeper;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZkClient {
    // 节点
    private final String connected = "192.168.44.133:2181,192.168.44.134:2181,192.168.44.135:2181";
    // 超时
    private int timeout = 2000;

    ZooKeeper zkCli = null;

    // 连接zookeeper集群
    @Before
    public void init() throws IOException {
        // String：连接集群的ip端口 Int:超时设置 Wacher:监听
        zkCli = new ZooKeeper(connected, timeout, new Watcher() {
            // 回调方法 显示/节点
            public void process(WatchedEvent watchedEvent) {
                List<String> children;
                // 获得节点 get
                try {
                    children = zkCli.getChildren("/",true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // 测试 是否连通集群 创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String str = zkCli.create("/mabin/yu","烤糊了".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        System.out.println("创建节点：" + str);
    }
}
