package com.longtao.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSMkdir {
    public static void main(String[] args) throws IOException {

        // 权限检查 指定root
        System.setProperty("HADOOP_USER_NAME","root");
        System.setProperty("hadoop.home.dir","C:\\Users\\Administrator\\Desktop\\大数据\\hadoop-2.4.1\\hadoop-2.4.1");
        // 配置参数指定namenode
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.44.128:9000");
        // 创建客户端
        FileSystem client = FileSystem.get(configuration);
        // 创建目录
        client.mkdirs(new Path("/test011322333"));
        client.close();
        System.out.println("Succesful!");

    }
}
