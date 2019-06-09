package com.longtao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HDFSDownLoad {
    public static void main(String[] args) throws IOException {
//        System.setProperty("HADOOP_USER_NAME","root");
//        // 配置环境变量HADOOP_HOME 或者指定Hadoop.home.dir 位置
//        System.setProperty("hadoop.home.dir","C:\\Users\\Administrator\\Desktop\\大数据\\hadoop-2.4.1\\hadoop-2.4.1");
        // 建立个客户端
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.44.128:9000");
        // 使用IP地址 没有指定bigdata1 对应的IP
        FileSystem client = FileSystem.get(configuration);

        InputStream inputStream = client.open(new Path("/test1/1.png"));

        OutputStream outputStream = new FileOutputStream("C:\\Users\\Administrator\\Desktop\\大数据\\tz\\2.png");
        // 写入本地 自动关闭流
        IOUtils.copyBytes(inputStream,outputStream,1024);

    }
}
