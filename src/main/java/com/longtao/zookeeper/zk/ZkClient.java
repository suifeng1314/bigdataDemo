package com.longtao.zookeeper.zk;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
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

    // 查看子节点
    @Test
    public void getClid() throws KeeperException, InterruptedException {
        List<String> children = zkCli.getChildren("/",true);
        for (String str : children){
            System.out.println(str);
        }
    }

    // 删除字节点数据: delete path
    @Test
    public void deleteData() throws KeeperException, InterruptedException {
        String path = "/lt";

        Stat exists = zkCli.exists(path,false);
        if (exists == null){
            System.out.println("节点不存在");
            return;
        }
        // -1 表示所有版本
        zkCli.delete(path,-1);
    }

    // 修改数据: set path data
    @Test
    public void setData() throws KeeperException, InterruptedException {
        zkCli.setData("/lt0000000004","nihao2".getBytes(),-1);
        // 查看
        byte[] data = zkCli.getData("/lt0000000004",false,new Stat());
        System.out.println(new String(data));
    }

    // 指点节点是否存在
    @Test
    public void ifExit() throws KeeperException, InterruptedException {
       Stat exists = zkCli.exists("/lt",false);
       System.out.println(exists == null ? "不存在" : "存在");
    }
}
