package com.longtao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class HDFSUpload {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME","root");
        // 配置环境变量HADOOP_HOME 或者指定Hadoop.home.dir 位置
        System.setProperty("hadoop.home.dir","C:\\Users\\Administrator\\Desktop\\大数据\\hadoop-2.4.1\\hadoop-2.4.1");
        // 建立个客户端
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.44.128:9000");
        // 使用IP地址 没有指定bigdata1 对应的IP
        FileSystem client = FileSystem.get(configuration);
        // 创建本地数据 hdfs dfs -put copyFromLocal
        File file = new File("C:\\Users\\Administrator\\Desktop\\img\\1.png");
        InputStream inputStream = new FileInputStream(file);
        // 创建本地输出流
        OutputStream outputStream = client.create(new Path("/test1/1.png"),true);
        // 写入HDFS
        byte[] buffer = new byte[1024];
        int len = 0;
        // 循环写入数据
        while ((len = inputStream.read(buffer)) != -1){
            outputStream.write(buffer,0,len);
        }
        // 防止数据写入不完整
        outputStream.flush();
        inputStream.close();
        outputStream.close();
        // 方法二 自动关闭流
        IOUtils.copyBytes(inputStream,outputStream,1024);

    }

}
