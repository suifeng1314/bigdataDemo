package com.longtao.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
//import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;


public class HbaseTest {
    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
    }

    // 1.判断表是否存在
    public static boolean isExist(String tableName) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        // 管理器 老版本:HBaseAdmin admin = new HBaseAdmin(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        boolean flag = admin.tableExists(TableName.valueOf(tableName));

        return flag;
    }

    // 创建表
    public static void createTable(String tableName,String... columnFamilly) throws IOException {
        // 1.管理器
        Connection connection = ConnectionFactory.createConnection(conf);

        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        // 2.创建描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        // 3.指定多个列簇
        for(String cf: columnFamilly){
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        // 4.创建表
        admin.createTable(hTableDescriptor);
        System.out.println("创建表成功！");

    }

    public static void main(String[] args) throws IOException {
        System.out.println("main~~~~~~~~");
//        boolean flag = isExist("student1");
//        System.out.println("表是否存在：" + flag);
        createTable("teacher","info1","info2","info3");

    }

}
