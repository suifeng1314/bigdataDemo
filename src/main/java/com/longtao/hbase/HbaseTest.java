package com.longtao.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
//import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


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

    // 2.创建表
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

    // 3.向表中添加数据 cd 列族
    public static void addData(String tableName,String rowKey,String cf,String clomn,String value) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 添加数据 put
        Put put = new Put(Bytes.toBytes(rowKey));
        // 指定表 列 值
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(clomn),Bytes.toBytes(value));

        table.put(put);
    }

    // 4.删除单个rowkey数据
    public static void deleteRow(String tableName,String rowkey) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowkey));

        table.delete(delete);
    }
    // 5.删除多个rowkey数据
    public static void deleteRows(String tableName,String... rowkey) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        List<Delete> list = new ArrayList<Delete>();
        for (String rk: rowkey){
            Delete delete = new Delete(Bytes.toBytes(rk));

            list.add(delete);
        }
        table.delete(list);
    }
    // 5.全表扫描
    public static void scanTable(String tableName) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        for (Result result:scanner){
            // 单元格
            Cell[] cells = result.rawCells();

            for (Cell cell: cells){
                System.out.println("rowkey:" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族:" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
    // 删除表
    public static void deleteTable(String tableName) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        admin.deleteTable(tableName);

        admin.deleteTable(TableName.valueOf(tableName));
    }

    public static void main(String[] args) throws IOException {
        System.out.println("main~~~~~~~~");
        boolean flag = isExist("student");
        System.out.println("表是否存在：" + flag);
//        createTable("teacher","info1","info2","info3");

    }

}
