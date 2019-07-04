package com.longtao.hbase.mr1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 读取hdfs数据
 */
public class ReadHdfsMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1.读取数据
        String line = value.toString();
        // 2.切分数据
        String[] fields = line.split("\t");
        // 3.封装数据
        byte[] rowkey = Bytes.toBytes(fields[0]);
        byte[] name = Bytes.toBytes(fields[1]);
        byte[] desc = Bytes.toBytes(fields[2]);
        // 4.封装成Put
        Put put = new Put(rowkey);
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),name);
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("desc"),desc);

        // 5.输入到reducer
        context.write(new ImmutableBytesWritable(rowkey),put);

    }
}
