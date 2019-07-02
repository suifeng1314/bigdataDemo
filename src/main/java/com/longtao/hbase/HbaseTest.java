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


    public static void main(String[] args) throws IOException {
        System.out.println(1);
        boolean flag = isExist("student1");
        System.out.println("表是否存在：" + flag);

    }

}
