package com.longtao.main.client;

import com.longtao.main.server.MyInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

public class MyRPCClient {
    public static void main(String[] args) throws Exception {
        // 配置环境变量HADOOP_HOME 或者指定Hadoop.home.dir 位置
        System.setProperty("hadoop.home.dir","C:\\Users\\Administrator\\Desktop\\大数据\\hadoop-2.4.1\\hadoop-2.4.1");
        // RPC调用server
        MyInterface proxy = RPC.getProxy(MyInterface.class,
                MyInterface.versionID,
                new InetSocketAddress("localhost",7770),
                new Configuration());

        String result = proxy.HelloWorld("longtao");
        System.out.println(result);
    }
}
