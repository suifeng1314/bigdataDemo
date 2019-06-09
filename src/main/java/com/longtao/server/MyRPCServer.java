package com.longtao.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;


public class MyRPCServer {
    public static void main(String[] args) throws Exception {
        // 配置环境变量HADOOP_HOME 或者指定Hadoop.home.dir 位置
        System.setProperty("hadoop.home.dir","C:\\Users\\Administrator\\Desktop\\大数据\\hadoop-2.4.1\\hadoop-2.4.1");
        // RPC框架实现通信
        RPC.Builder builder = new RPC.Builder(new Configuration());
        // 定义server参数(本地)
        builder.setBindAddress("localhost");
        builder.setPort(7770);
        // 部署程序
        builder.setProtocol(MyInterface.class);
        builder.setInstance(new MyInterfaceImpl());
        // 开启server
        Server server = builder.build();
        server.start();
    }
}
